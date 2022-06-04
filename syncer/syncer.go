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
	fileName util.RelPathType
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

func (de *diffFileEntry) getAction(s *syncer) action {
	bothMetasPresent := de.local != nil && de.remote != nil
	noOp := bothMetasPresent && de.local.Md5sum == de.remote.Md5
	if !noOp {
		log.Printf("diffEntry - %v %v", de.fileName, de.String())
	}
	if de.localChange == changeTypeRem && de.remoteChange == changeTypeRem {
		// Removed from both remote and local.
		return nil
	} else if de.remoteChange == changeTypeRem { // removed from remote
		if de.localChange == changeTypeUpdated {
			// locally the file is updated or added. It is removed from remote.
			// lets play safe and add it back to remote.
			return &blobWrite{
				localBasePath: s.localBasePath,
				relativePath:  de.fileName,
				backend:       s.backend,
			}
		} else if de.local != nil {
			// no change on local => remove on local
			return &localRemove{
				basePath:         s.localBasePath,
				relativeFilePath: de.fileName,
			}
		}
	} else if de.localChange == changeTypeRem { // removed from local
		if de.remoteChange == changeTypeUpdated { // update on blobstore
			blobWriterClientId := de.remote.BlobWriterClientId
			if blobWriterClientId != nil && *blobWriterClientId == util.UniqueMachineId {
				// 1. Source of the blob is the current machine
				// 2. Blob is not on the machine
				// => blob was removed from the machine after it was uploaded => So we need to remove the blob
				return &blobRemove{
					relativeFilePath: de.fileName,
					backend:          s.backend,
				}
			} else {
				// locally file is removed. But it updated on remote recently.
				// lets play safe and add it back to local
				return &localWrite{
					localBasePath: s.localBasePath,
					relativePath:  de.fileName,
					backend:       s.backend,
					blobInfo:      de.remote,
				}
			}
		} else if de.remote != nil {
			// no changes on remote => remove on remote
			return &blobRemove{
				relativeFilePath: de.fileName,
				backend:          s.backend,
			}
		}
		return nil
	} else if !bothMetasPresent {
		// This will happen if the file is present only on one side => (typically file add)
		if de.local != nil {
			return &blobWrite{
				localBasePath: s.localBasePath,
				relativePath:  de.fileName,
				backend:       s.backend,
				localMeta:     de.local,
				remoteMeta:    de.remote,
			}
		} else {
			return &localWrite{
				localBasePath: s.localBasePath,
				relativePath:  de.fileName,
				backend:       s.backend,
				blobInfo:      de.remote,
			}
		}
	} else {
		// We have both the metadatas. Lets check which one is recent and write to the other.
		if de.local.Md5sum == de.remote.Md5 {
			// file changed. But same md5 in both places.
			return nil
		} else if de.local.ModTime.After(de.remote.ModTime) {
			// local timestamp higher => write to remote
			return &blobWrite{
				localBasePath: s.localBasePath,
				relativePath:  de.fileName,
				backend:       s.backend,
				localMeta:     de.local,
				remoteMeta:    de.remote,
			}
		} else {
			// remote timestamp higher => write to local
			return &localWrite{
				localBasePath: s.localBasePath,
				relativePath:  de.fileName,
				backend:       s.backend,
				blobInfo:      de.remote,
			}
		}
	}
	return nil
}

type diffFromLastRunState map[util.RelPathType]*diffFileEntry

func (d *diffFromLastRunState) setLocalDiff(key util.RelPathType, local *util.LocalFileMeta, ct changeType) {
	entry, ok := (*d)[key]
	if ok {
		entry.localChange = ct
		entry.local = local
	} else {
		(*d)[key] = &diffFileEntry{
			fileName:     key,
			local:        local,
			localChange:  ct,
			remoteChange: changeTypeNone,
		}
	}
}

func (d *diffFromLastRunState) setRemoteDiff(key util.RelPathType, remote *blob.MetaEntry, ct changeType) {
	entry, ok := (*d)[key]
	if ok {
		entry.remote = remote
		entry.remoteChange = ct
	} else {
		(*d)[key] = &diffFileEntry{
			fileName:     key,
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
	state := make(diffFromLastRunState)
	newLocalFiles := make(map[util.RelPathType]util.LocalFileMeta)
	for _, _lm := range newRun.local {
		localMeta := _lm
		newLocalFiles[_lm.RelPath] = _lm
		state.setLocalDiff(localMeta.RelPath, &localMeta, changeTypeNone)
	}
	for _, _oldFile := range s.lastScan.local {
		newFile, ok := newLocalFiles[_oldFile.RelPath]
		if !ok {
			state.setLocalDiff(_oldFile.RelPath, nil, changeTypeRem)
		} else {
			if newFile.Md5sum != _oldFile.Md5sum {
				state.setLocalDiff(newFile.RelPath, &newFile, changeTypeUpdated)
			}
			// Delete the file from newLocalFiles map. After this for loop
			// the entries left in this map are newly added.
			delete(newLocalFiles, newFile.RelPath)
		}
	}
	for _, _newFile := range newLocalFiles {
		// all these are newly added.
		newFile := _newFile
		state.setLocalDiff(_newFile.RelPath, &newFile, changeTypeUpdated)
	}

	newFilesRemote := make(map[util.RelPathType]blob.MetaEntry)
	for _, _remoteFile := range newRun.remote {
		remoteFile := _remoteFile
		newFilesRemote[_remoteFile.RelPath] = _remoteFile
		state.setRemoteDiff(_remoteFile.RelPath, &remoteFile, changeTypeNone)
	}
	for _, _oldRemoteFile := range s.lastScan.remote {
		newRemoteFile, ok := newFilesRemote[_oldRemoteFile.RelPath]
		if !ok {
			state.setRemoteDiff(_oldRemoteFile.RelPath, nil, changeTypeRem)
		} else {
			if newRemoteFile.Md5 != _oldRemoteFile.Md5 {
				state.setRemoteDiff(newRemoteFile.RelPath, &newRemoteFile, changeTypeUpdated)
			}
			delete(newFilesRemote, newRemoteFile.RelPath)
		}
	}
	for _, _newRemoteFile := range newFilesRemote {
		newRemoteFile := _newRemoteFile
		state.setRemoteDiff(_newRemoteFile.RelPath, &newRemoteFile, changeTypeUpdated)
	}

	ret := make([]action, 0)
	for _, entry := range state {
		fileAction := entry.getAction(s)
		if fileAction != nil {
			ret = append(ret, fileAction)
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
