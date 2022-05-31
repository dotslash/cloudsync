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
	remote   map[util.RelPathType]blob.MetaEntry
	local    map[util.RelPathType]util.LocalFileMeta
	scanTime time.Time
}

// TODO: Remove??
func (s *ScanResult) isDefault() bool {
	return s.scanTime == time.Time{}
}

type syncer struct {
	localBasePath string
	backend       blob.Backend
	lastScan      ScanResult
}

type changeType string

const (
	changeTypeUpdated changeType = "update"
	changeTypeRem     changeType = "rem"
	changeTypeNone    changeType = "none"
)

type diffFileEntry struct {
	// if localChanged is false, no change on local
	// if localChanged is true, localDiff has this semantic
	//   nil => no diff.
	//   Otherwise, the file got changed/added
	local       *util.LocalFileMeta
	localChange changeType
	// if remoteChanged is false, no change on local
	// if remoteChanged is true, remoteDiff has this semantic
	//   nil => no diff.
	//   Otherwise, the file got changed/added
	remote       *blob.MetaEntry
	remoteChange changeType
}

func (de diffFileEntry) String() string {
	localMd5, remoteMd5 := "na", "na"
	if de.local != nil {
		localMd5 = de.local.Md5sum
	}
	if de.remote != nil {
		remoteMd5 = de.remote.Md5
	}
	return fmt.Sprintf("local:%v@%v remote:%v@%v", de.localChange, localMd5, de.remoteChange, remoteMd5)
}

type diffFromLastRunRes map[util.RelPathType]*diffFileEntry

func (d *diffFromLastRunRes) setLocalDiff(key util.RelPathType, local *util.LocalFileMeta, ct changeType) {
	entry, ok := (*d)[key]
	if ok {
		entry.localChange = ct
		entry.local = local
	} else {
		(*d)[key] = &diffFileEntry{
			local:        local,
			localChange:  ct,
			remoteChange: changeTypeNone,
		}
	}
}

func (d *diffFromLastRunRes) setRemoteDiff(key util.RelPathType, remote *blob.MetaEntry, ct changeType) {
	entry, ok := (*d)[key]
	if ok {
		entry.remote = remote
		entry.remoteChange = ct
	} else {
		(*d)[key] = &diffFileEntry{
			remote:       remote,
			remoteChange: ct,
			localChange:  changeTypeNone,
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
		bothMetasPresent := diffEntry.local != nil && diffEntry.remote != nil
		if diffEntry.localChange == changeTypeRem && diffEntry.remoteChange == changeTypeRem {
			// Removed from both remote and local.
			continue
		} else if diffEntry.remoteChange == changeTypeRem { // removed from remote
			if diffEntry.localChange == changeTypeUpdated {
				// locally the file is updated or added. It is removed from remote.
				// lets play safe and add it back to remote.
				ret = append(ret, &blobWrite{
					localBasePath: s.localBasePath,
					relativePath:  fn,
					backend:       s.backend,
				})
			} else if diffEntry.local != nil {
				// no change on local => remove on local
				ret = append(ret, &localRemove{
					basePath:         s.localBasePath,
					relativeFilePath: fn,
				})
			}
		} else if diffEntry.localChange == changeTypeRem { // removed from local
			if diffEntry.remoteChange == changeTypeUpdated { // update on blobstore
				blobWriterClientId := diffEntry.remote.BlobWriterClientId
				if blobWriterClientId != nil && *blobWriterClientId == util.UniqueMachineId {
					// 1. Source of the blob is the current machine
					// 2. Blob is not on the machine
					// => blob was removed from the machine after it was uploaded => So we need to remove the blob
					ret = append(ret, &blobRemove{
						relativeFilePath: fn,
						backend:          s.backend,
					})
				} else {
					// locally file is removed. But it updated on remote recently.
					// lets play safe and add it back to local
					ret = append(ret, &localWrite{
						localBasePath: s.localBasePath,
						relativePath:  fn,
						backend:       s.backend,
						blobInfo:      diffEntry.remote,
					})
				}
			} else if diffEntry.remote != nil {
				// no changes on remote => remove on remote
				ret = append(ret, &blobRemove{
					relativeFilePath: fn,
					backend:          s.backend,
				})
			}
		} else if !bothMetasPresent {
			// This will happen if the file is present only on one side => (typically file add)
			if diffEntry.local != nil {
				ret = append(ret, &blobWrite{
					localBasePath: s.localBasePath,
					relativePath:  fn,
					backend:       s.backend,
					localMeta:     diffEntry.local,
					remoteMeta:    diffEntry.remote,
				})
			} else {
				ret = append(ret, &localWrite{
					localBasePath: s.localBasePath,
					relativePath:  fn,
					backend:       s.backend,
					blobInfo:      diffEntry.remote,
				})
			}
		} else if diffEntry.localChange == changeTypeUpdated || diffEntry.remoteChange == changeTypeUpdated {
			// change on both remote and local and both metadata entries present
			if diffEntry.local.Md5sum == diffEntry.remote.Md5 {
				// file changed. But same md5 in both places.
				continue
			} else if diffEntry.local.ModTime.After(diffEntry.remote.ModTime) {
				// local timestamp higher => write to remote
				ret = append(ret, &blobWrite{
					localBasePath: s.localBasePath,
					relativePath:  fn,
					backend:       s.backend,
					localMeta:     diffEntry.local,
					remoteMeta:    diffEntry.remote,
				})
			} else {
				// remote timestamp higher => write to local
				ret = append(ret, &localWrite{
					localBasePath: s.localBasePath,
					relativePath:  fn,
					backend:       s.backend,
					blobInfo:      diffEntry.remote,
				})
			}
		} else {
			log.Printf("getActions: This should not happen %v %v", fn, diffEntry.String())
		}
	}
	return ret
}

func (s *syncer) diffFromLastRun(newRun *ScanResult) diffFromLastRunRes {
	ret := make(diffFromLastRunRes)
	newLocalFiles := make(map[util.RelPathType]util.LocalFileMeta)
	for _, _lm := range newRun.local {
		localMeta := _lm
		newLocalFiles[_lm.RelPath] = _lm
		ret.setLocalDiff(localMeta.RelPath, &localMeta, changeTypeNone)
	}
	for _, _oldFile := range s.lastScan.local {
		newFile, ok := newLocalFiles[_oldFile.RelPath]
		if !ok {
			ret.setLocalDiff(_oldFile.RelPath, nil, changeTypeRem)
		} else {
			if newFile.Md5sum != _oldFile.Md5sum {
				ret.setLocalDiff(newFile.RelPath, &newFile, changeTypeUpdated)
			}
			// Delete the file from newLocalFiles map. After this for loop
			// the entries left in this map are newly added.
			delete(newLocalFiles, newFile.RelPath)
		}
	}
	for _, _newFile := range newLocalFiles {
		// all these are newly added.
		newFile := _newFile
		ret.setLocalDiff(_newFile.RelPath, &newFile, changeTypeUpdated)
	}

	newFilesRemote := make(map[util.RelPathType]blob.MetaEntry)
	for _, _remoteFile := range newRun.remote {
		remoteFile := _remoteFile
		newFilesRemote[_remoteFile.RelPath] = _remoteFile
		ret.setRemoteDiff(_remoteFile.RelPath, &remoteFile, changeTypeNone)
	}
	for _, _oldRemoteFile := range s.lastScan.remote {
		newRemoteFile, ok := newFilesRemote[_oldRemoteFile.RelPath]
		if !ok {
			ret.setRemoteDiff(_oldRemoteFile.RelPath, nil, changeTypeRem)
		} else {
			if newRemoteFile.Md5 != _oldRemoteFile.Md5 {
				ret.setRemoteDiff(newRemoteFile.RelPath, &newRemoteFile, changeTypeUpdated)
			}
			delete(newFilesRemote, newRemoteFile.RelPath)
		}
	}
	for _, _newRemoteFile := range newFilesRemote {
		newRemoteFile := _newRemoteFile
		ret.setRemoteDiff(_newRemoteFile.RelPath, &newRemoteFile, changeTypeUpdated)
	}

	for rp, entry := range ret {
		if entry.localChange == changeTypeNone && entry.remoteChange == changeTypeNone {
			delete(ret, rp)
		}
	}

	return ret
}

func (s *syncer) syncCore() error {
	var err error
	log.Printf("syncCore.start->==================================")
	defer func() {
		if err != nil {
			log.Printf("syncCore.done(err)->==================================")
		} else {
			log.Printf("syncCore.done(ok)->==================================")
		}
	}()
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
	log.Printf("s.applyChanges done. numActions %v", len(actions))
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
