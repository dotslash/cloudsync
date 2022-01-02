package main

import (
	"fmt"
	"github.com/dotslash/cloudsync/blob"
	"github.com/dotslash/cloudsync/util"
	"log"
	"os"
	"path"
	"time"
)

type ScanResult struct {
	remote []blob.MetaEntry
	local  []util.LocalFileMeta
}

type syncer struct {
	localPath string
	backend   blob.Backend
	lastScan  *ScanResult
}

type localRemoteDiff struct {
	remoteToRemove []string
	remoteToWrite  []string
	localToRemove  []string
	localToWrite   []string
}

func (s *syncer) start() {
	s.syncLoop()
}

func (s *syncer) syncCore() {
	remoteFiles, _ := s.backend.ListDirRecursive("")
	localFiles, _ := util.ListFilesRec(s.localPath)
	res := &ScanResult{remote: remoteFiles, local: localFiles}
	type Info struct {
		blob  *blob.MetaEntry
		local *util.LocalFileMeta
	}
	pathToInfo := make(map[string]Info)
	for _, r := range remoteFiles {
		pathToInfo[r.RelPath] = Info{blob: &r}
	}
	for _, l := range localFiles {
		if info, ok := pathToInfo[l.RelPath]; ok {
			pathToInfo[l.RelPath] = Info{local: &l, blob: info.blob}
		} else {
			pathToInfo[l.RelPath] = Info{local: &l}
		}
	}
	diff := localRemoteDiff{}
	for p, info := range pathToInfo {
		if info.blob == nil {
			diff.remoteToWrite = append(diff.remoteToWrite, p)
		} else if info.local == nil {
			diff.localToWrite = append(diff.localToWrite, p)
		} else if info.local.Md5sum == info.blob.Md5 {
			// pass
		} else if info.local.ModTime.After(info.blob.ModTime) {
			diff.remoteToWrite = append(diff.remoteToWrite, p)
		} else {
			diff.localToWrite = append(diff.localToWrite, p)
		}
	}
	_ = s.applyChanges(diff)
	s.lastScan = res
}

func (s *syncer) scheduleNextSync() {
	time.Sleep(30 * time.Second)
	s.syncLoop()
}
func (s *syncer) syncLoop() {
	s.syncCore()
	go s.scheduleNextSync()
}

func (s *syncer) applyChanges(diff localRemoteDiff) error {
	fmt.Println(diff.remoteToWrite)
	fmt.Println(diff.localToWrite)
	for _, f := range diff.remoteToWrite {
		if err := s.writeToRemote(f); err != nil {
			return err
		}
	}

	for _, f := range diff.localToWrite {
		if err := s.writeToLocal(f); err != nil {
			return err
		}
	}
	return nil
}

func (s *syncer) writeToRemote(f string) error {
	log.Printf("Writing from %v to remote:%v", path.Join(s.localPath, f), f)
	file, err := os.Open(path.Join(s.localPath, f))
	if err != nil {
		return fmt.Errorf("remoteToWrite: Open(%v %v) failed - %e", s.localPath, f, err)
	}
	if err = s.backend.Put(f, file); err != nil {
		return fmt.Errorf("remoteToWrite: Put(%v) failed - %e", f, err)
	}
	return nil
}

func (s *syncer) writeToLocal(f string) error {
	log.Printf("Writing from remote:%v to %v", f, path.Join(s.localPath, f))
	localPathForCurFile := path.Join(s.localPath, f)
	if err := os.MkdirAll(path.Dir(localPathForCurFile), 0755); err != nil {
		return fmt.Errorf("localToWrite: MkdirAll(%v) failed - %e", path.Dir(localPathForCurFile), err)
	}
	if file, err := os.OpenFile(path.Join(s.localPath, f), os.O_CREATE|os.O_RDWR, 0755); err != nil {
		return fmt.Errorf("localToWrite: OpenFile(%v) failed - %e", localPathForCurFile, err)
	} else if blobEntry, err := s.backend.Get(f); err != nil {
		return fmt.Errorf("localToWrite: backend.Get(%v) failed - %e", f, err)
	} else if err = util.CopyAndClose(file, blobEntry.Content); err != nil {
		return fmt.Errorf("localToWrite: CopyAndClose(%v) failed - %e", f, err)
	}
	return nil
}

func newSyncer(localPath string, backend blob.Backend) *syncer {
	return &syncer{localPath: localPath, backend: backend}
}
