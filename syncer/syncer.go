package syncer

import (
	"fmt"
	"github.com/dotslash/cloudsync/blob"
	"github.com/dotslash/cloudsync/util"
	"log"
	"time"
)

// ScanResult contains the information present on remote and local filesystem
// about what files are present and their metadata.
type ScanResult struct {
	remote   []blob.MetaEntry
	local    []util.LocalFileMeta
	scanTime time.Time
}

type syncer struct {
	localBasePath string
	backend       blob.Backend
	lastScan      ScanResult
}

type diffFileEntry struct {
	// nil => no diff. Otherwise, the file got changed/added
	localDiff   *util.LocalFileMeta
	localChange bool
	// nil => no diff. Otherwise, the file got changed/added
	remoteDiff   *blob.MetaEntry
	remoteChange bool
}

func (de *diffFileEntry) localRemoved() bool {
	return de.localChange && de.localDiff == nil
}

func (de *diffFileEntry) remoteRemoved() bool {
	return de.remoteChange && de.remoteDiff == nil
}

type diffFromLastRunRes map[string]*diffFileEntry

func (d *diffFromLastRunRes) setLocal(key string, local *util.LocalFileMeta) {
	entry, ok := (*d)[key]
	if ok {
		entry.localDiff = local
		entry.localChange = true
	} else {
		(*d)[key] = &diffFileEntry{
			localDiff:   local,
			localChange: true,
		}
	}
}

func (d *diffFromLastRunRes) setRemote(key string, remote *blob.MetaEntry) {
	entry, ok := (*d)[key]
	if ok {
		entry.remoteDiff = remote
		entry.remoteChange = true
	} else {
		(*d)[key] = &diffFileEntry{
			remoteDiff:   remote,
			remoteChange: true,
		}
	}
}

func (s *syncer) Start() {
	for {
		err := s.syncCore()
		if err != nil {
			log.Printf("syncCore failed. err=%v", err)
		}
		time.Sleep(30 * time.Second)
	}
}

func (s *syncer) getActions(newRun *ScanResult) []action {
	diff := s.diffFromLastRun(newRun)
	ret := make([]action, 0)
	for fn, diffEntry := range diff {
		if diffEntry.remoteRemoved() && diffEntry.localRemoved() {
			// Removed from both remote and local.
			continue
		} else if diffEntry.remoteRemoved() { // removed from remote
			if diffEntry.localDiff != nil {
				// locally the file is updated or added. It is removed from remote.
				// lets play safe and add it back to remote.
				ret = append(ret, &blobWrite{
					localBasePath: s.localBasePath,
					relativePath:  fn,
					backend:       s.backend,
				})
			} else {
				// no change on local => remove on local
				ret = append(ret, &localRemove{
					basePath:         s.localBasePath,
					relativeFilePath: fn,
				})
			}
		} else if diffEntry.localRemoved() { // removed from local
			if diffEntry.remoteDiff != nil {
				// locally file is removed. But it updated on remote recently.
				// lets play safe and add it back to local
				ret = append(ret, &localWrite{
					localBasePath: s.localBasePath,
					relativePath:  fn,
					backend:       s.backend,
				})
			}
		} else if diffEntry.localChange && diffEntry.remoteChange { // change on both remote and local
			if diffEntry.localDiff.Md5sum == diffEntry.remoteDiff.Md5 {
				// file changed. But same md5 in both places.
				continue
			} else if diffEntry.localDiff.ModTime.After(diffEntry.remoteDiff.ModTime) {
				// local timestamp higher => write to remote
				ret = append(ret, &blobWrite{
					localBasePath: s.localBasePath,
					relativePath:  fn,
					backend:       s.backend,
				})
			} else {
				// remote timestamp higher => write to local
				ret = append(ret, &localWrite{
					localBasePath: s.localBasePath,
					relativePath:  fn,
					backend:       s.backend,
				})
			}
		} else if diffEntry.localChange {
			// local change only => update to remote
			ret = append(ret, &blobWrite{
				localBasePath: s.localBasePath,
				relativePath:  fn,
				backend:       s.backend,
			})
		} else if diffEntry.remoteChange {
			// remote change only => update to local
			ret = append(ret, &localWrite{
				localBasePath: s.localBasePath,
				relativePath:  fn,
				backend:       s.backend,
			})
		} else {
			log.Printf("getActions: This should not happen %v %#v", fn, diffEntry)
		}
	}
	return nil
}

func (s *syncer) diffFromLastRun(newRun *ScanResult) diffFromLastRunRes {
	ret := make(diffFromLastRunRes)
	newLocalFiles := make(map[string]util.LocalFileMeta)
	for _, lm := range newRun.local {
		newLocalFiles[lm.RelPath] = lm
	}
	for _, oldFile := range s.lastScan.local {
		newFile, ok := newLocalFiles[oldFile.RelPath]
		if !ok {
			ret.setLocal(oldFile.RelPath, nil)
		} else if newFile.Md5sum != oldFile.Md5sum {
			ret.setLocal(newFile.RelPath, &newFile)
			delete(newLocalFiles, newFile.RelPath)
		}
	}
	for _, newFile := range newLocalFiles {
		ret.setLocal(newFile.RelPath, &newFile)
	}

	newFilesRemote := make(map[string]blob.MetaEntry)
	for _, remoteFile := range newRun.remote {
		newFilesRemote[remoteFile.RelPath] = remoteFile
	}
	for _, oldRemoteFile := range s.lastScan.remote {
		newRemoteFile, ok := newFilesRemote[oldRemoteFile.RelPath]
		if !ok {
			ret.setRemote(oldRemoteFile.RelPath, nil)
		} else if newRemoteFile.Md5 != oldRemoteFile.Md5 {
			ret.setRemote(newRemoteFile.RelPath, &newRemoteFile)
			delete(newFilesRemote, newRemoteFile.RelPath)
		}
	}
	for _, newRemoteFile := range newFilesRemote {
		ret.setRemote(newRemoteFile.RelPath, &newRemoteFile)
	}

	return ret
}

func (s *syncer) syncCore() error {
	remoteFiles, err := s.backend.ListDirRecursive("")
	if err != nil {
		return err
	}
	localFiles, err := util.ListFilesRec(s.localBasePath)
	if err != nil {
		return err
	}
	scanRes := &ScanResult{remote: remoteFiles, local: localFiles, scanTime: time.Now()}
	type Info struct {
		blob  *blob.MetaEntry
		local *util.LocalFileMeta
	}
	pathToInfo := make(map[string]*Info)
	for i := range remoteFiles {
		r := remoteFiles[i]
		pathToInfo[r.RelPath] = &Info{blob: &r}
	}
	for i := range localFiles {
		l := localFiles[i]
		if info, ok := pathToInfo[l.RelPath]; ok {
			info.local = &l
		} else {
			pathToInfo[l.RelPath] = &Info{local: &l}
		}
	}
	actions := make([]action, 0)
	for p, info := range pathToInfo {
		if info.blob == nil {
			actions = append(actions, &blobWrite{
				localBasePath: s.localBasePath,
				relativePath:  p,
				backend:       s.backend,
			})
		} else if info.local == nil {
			actions = append(actions, &localWrite{
				localBasePath: s.localBasePath,
				relativePath:  p,
				backend:       s.backend,
			})
		} else if info.local.Md5sum == info.blob.Md5 {
			// pass
		} else if info.local.ModTime.After(info.blob.ModTime) {
			actions = append(actions, &blobWrite{
				localBasePath: s.localBasePath,
				relativePath:  p,
				backend:       s.backend,
			})
		} else {
			actions = append(actions, &localWrite{
				localBasePath: s.localBasePath,
				relativePath:  p,
				backend:       s.backend,
			})
		}
	}
	err = s.applyChanges(actions)
	s.lastScan = scanRes
	return err
}

func (s *syncer) applyChanges(actions []action) error {
	for _, a := range actions {
		if err := a.do(); err != nil {
			return fmt.Errorf("failure in %v", a)
		}
	}
	return nil
}

func NewSyncer(localPath string, _ string, backend blob.Backend) *syncer {
	return &syncer{localBasePath: localPath, backend: backend}
}
