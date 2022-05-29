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
	// if localChanged is false, no change on local
	// if localChanged is true, localDiff has this semantic
	//   nil => no diff.
	//   Otherwise, the file got changed/added
	localDiff    *util.LocalFileMeta
	localChanged bool
	// if remoteChanged is false, no change on local
	// if remoteChanged is true, remoteDiff has this semantic
	//   nil => no diff.
	//   Otherwise, the file got changed/added
	remoteDiff    *blob.MetaEntry
	remoteChanged bool
}

func (de *diffFileEntry) localRemoved() bool {
	return de.localChanged && de.localDiff == nil
}

func (de *diffFileEntry) remoteRemoved() bool {
	return de.remoteChanged && de.remoteDiff == nil
}

func (de diffFileEntry) String() string {
	localChange, remoteChange := "N/A", "N/A"
	if de.localRemoved() {
		localChange = "REM"
	} else if de.localChanged {
		localChange = fmt.Sprintf("UPDATED %v", de.localDiff.Md5sum)
	}
	if de.remoteRemoved() {
		remoteChange = "REM"
	} else if de.remoteChanged {
		remoteChange = fmt.Sprintf("UPDATED %v", de.remoteDiff.Md5)
	}
	return fmt.Sprintf("local:%v remote:%v", localChange, remoteChange)
}

type diffFromLastRunRes map[string]*diffFileEntry

func (d *diffFromLastRunRes) setLocalDiff(key string, local *util.LocalFileMeta) {
	entry, ok := (*d)[key]
	if ok {
		entry.localDiff = local
		entry.localChanged = true
	} else {
		(*d)[key] = &diffFileEntry{
			localDiff:    local,
			localChanged: true,
		}
	}
}

func (d *diffFromLastRunRes) setRemoteDiff(key string, remote *blob.MetaEntry) {
	entry, ok := (*d)[key]
	if ok {
		entry.remoteDiff = remote
		entry.remoteChanged = true
	} else {
		(*d)[key] = &diffFileEntry{
			remoteDiff:    remote,
			remoteChanged: true,
		}
	}
}

func (s *syncer) Start() {
	for {
		log.Printf("Starting syncCode")
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
		log.Printf("diffEntry - %v %v", fn, diffEntry.String())
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
					blobInfo:      diffEntry.remoteDiff,
				})
			} else {
				// no changes on remote => remove on remote
				ret = append(ret, &blobRemove{
					relativeFilePath: fn,
					backend:          s.backend,
				})
			}
		} else if diffEntry.localChanged && diffEntry.remoteChanged { // change on both remote and local
			//log.Printf("diffEntry(local, remote): %#v %#v",
			//	*diffEntry.localDiff,
			//	*diffEntry.remoteDiff)
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
				isRemoteUpdatedRecently := diffEntry.remoteDiff.ModTime.Before(time.Now().Add(-1 * time.Minute))
				if isRemoteUpdatedRecently {
					// Download from remote only if remote's timestamp is 1min higher than local. This is to avoid
					// concurrency issues where data is modified locally actively.
					log.Printf("Remote updated less than 1min ago. Will skip the download this time. %v %v",
						fn,
						diffEntry.remoteDiff.ModTime)
				} else {
					// remote timestamp higher => write to local
					ret = append(ret, &localWrite{
						localBasePath: s.localBasePath,
						relativePath:  fn,
						backend:       s.backend,
						blobInfo:      diffEntry.remoteDiff,
					})
				}
			}
		} else if diffEntry.localChanged {
			// local change only => update to remote
			ret = append(ret, &blobWrite{
				localBasePath: s.localBasePath,
				relativePath:  fn,
				backend:       s.backend,
			})
		} else if diffEntry.remoteChanged {
			// remote change only => update to local
			ret = append(ret, &localWrite{
				localBasePath: s.localBasePath,
				relativePath:  fn,
				backend:       s.backend,
				blobInfo:      diffEntry.remoteDiff,
			})
		} else {
			log.Printf("getActions: This should not happen %v %#v", fn, diffEntry)
		}
	}
	return ret
}

func (s *syncer) diffFromLastRun(newRun *ScanResult) diffFromLastRunRes {
	ret := make(diffFromLastRunRes)
	newLocalFiles := make(map[string]util.LocalFileMeta)
	for _, _lm := range newRun.local {
		newLocalFiles[_lm.RelPath] = _lm
	}
	for _, _oldFile := range s.lastScan.local {
		newFile, ok := newLocalFiles[_oldFile.RelPath]
		if !ok {
			ret.setLocalDiff(_oldFile.RelPath, nil)
		} else {
			if newFile.Md5sum != _oldFile.Md5sum {
				ret.setLocalDiff(newFile.RelPath, &newFile)
			}
			delete(newLocalFiles, newFile.RelPath)
		}
	}
	for _, _newFile := range newLocalFiles {
		newFile := _newFile
		ret.setLocalDiff(_newFile.RelPath, &newFile)
	}

	newFilesRemote := make(map[string]blob.MetaEntry)
	for _, _remoteFile := range newRun.remote {
		newFilesRemote[_remoteFile.RelPath] = _remoteFile
	}
	for _, _oldRemoteFile := range s.lastScan.remote {
		newRemoteFile, ok := newFilesRemote[_oldRemoteFile.RelPath]
		if !ok {
			ret.setRemoteDiff(_oldRemoteFile.RelPath, nil)
		} else {
			if newRemoteFile.Md5 != _oldRemoteFile.Md5 {
				ret.setRemoteDiff(newRemoteFile.RelPath, &newRemoteFile)
			}
			delete(newFilesRemote, newRemoteFile.RelPath)
		}
	}
	for _, _newRemoteFile := range newFilesRemote {
		newRemoteFile := _newRemoteFile
		ret.setRemoteDiff(_newRemoteFile.RelPath, &newRemoteFile)
	}

	return ret
}

func (s *syncer) syncCore() error {
	log.Printf("==================================")
	log.Printf("==================================")
	remoteFiles, err := s.backend.ListDirRecursive("")
	if err != nil {
		return err
	}
	log.Printf("backend.ListDirRecursive done")
	localFiles, err := util.ListFilesRec(s.localBasePath)
	if err != nil {
		return err
	}
	log.Printf("util.ListFilesRec done")
	scanRes := ScanResult{remote: remoteFiles, local: localFiles, scanTime: time.Now()}
	actions := s.getActions(&scanRes)
	log.Printf("s.getActions done. numActions %v", len(actions))
	err = s.applyChanges(actions)
	// log.Printf("s.applyChanges done. numActions %v", len(actions))
	s.lastScan = scanRes
	return err
}

func (s *syncer) applyChanges(actions []action) error {
	for _, a := range actions {
		if err := a.do(); err != nil {
			// TODO: Dont fail the entire thing if one operation fails.
			return fmt.Errorf("failure in %v", a)
		}
	}
	return nil
}

func NewSyncer(localPath string, _ string, backend blob.Backend) *syncer {
	return &syncer{localBasePath: localPath, backend: backend}
}
