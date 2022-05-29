package syncer

import (
	"fmt"
	"github.com/dotslash/cloudsync/blob"
	"github.com/dotslash/cloudsync/util"
	"log"
	"os"
	"path"
)

type action interface {
	do() error
}

type localRemove struct {
	basePath         string
	relativeFilePath string
}

func (lr *localRemove) do() error {
	fullPath := path.Join(lr.basePath, lr.relativeFilePath)
	log.Printf("localRemove: Removing %v full:%v", lr.relativeFilePath, fullPath)
	return os.Remove(fullPath)
}

func (lr *localRemove) String() string {
	return fmt.Sprintf("localRemove:%v", lr.relativeFilePath)
}

type blobRemove struct {
	relativeFilePath string
	backend          blob.Backend
}

func (s *blobRemove) do() error {
	log.Printf("blobRemove: Removing %v", s.relativeFilePath)
	return s.backend.Delete(s.relativeFilePath)
}

func (br *blobRemove) String() string {
	return fmt.Sprintf("blobRemove:%v", br.relativeFilePath)
}

type blobWrite struct {
	localBasePath string
	relativePath  string
	backend       blob.Backend
}

// TODO: Dont do the write if the target file exists already and has a higher timestamp
func (bw *blobWrite) do() error {
	localFullPath := path.Join(bw.localBasePath, bw.relativePath)
	log.Printf("Writing from %v to remote:%v", localFullPath, bw.relativePath)
	file, err := os.Open(localFullPath)
	if err != nil {
		return fmt.Errorf("remoteToWrite: Open(%v %v) failed - %e", localFullPath, bw.relativePath, err)
	}
	if err = bw.backend.Put(bw.relativePath, file); err != nil {
		return fmt.Errorf("remoteToWrite: Put(%v) failed - %e", bw.relativePath, err)
	}
	return nil
}
func (bw *blobWrite) String() string {
	return fmt.Sprintf("blobWrite:%v", bw.relativePath)
}

type localWrite struct {
	localBasePath string
	relativePath  string
	backend       blob.Backend
	blobInfo      *blob.MetaEntry
}

// TODO: Dont do the write if the target file exists already and has a higher timestamp
func (lw *localWrite) do() error {
	localFullPath := path.Join(lw.localBasePath, lw.relativePath)
	ctxString := fmt.Sprintf("localWrite(%v)", lw.relativePath)
	log.Printf("[%v] Starting remote:%v to %v", ctxString, lw.relativePath, localFullPath)
	// TODO: maybe handle error. Here i only care about the case where info is ready
	info, err := util.GetLocalFileMeta(lw.localBasePath, lw.relativePath)
	if err == nil && info.Md5sum == lw.blobInfo.Md5 {
		// test
		log.Printf("[%v] Local file's md5 sum is same. Skipping the localWrite", ctxString)
		return nil
	} else if err == nil && info.ModTime.After(lw.blobInfo.ModTime) {
		log.Printf("[%v] Local file is modified after remote. Skipping the localWrite", ctxString)
		return nil
	} else if err := os.MkdirAll(path.Dir(localFullPath), 0755); err != nil {
		return fmt.Errorf("[%v] MkdirAll(%v) failed - %e", ctxString, path.Dir(localFullPath), err)
	} else if file, err := os.OpenFile(localFullPath, os.O_CREATE|os.O_RDWR, 0755); err != nil {
		return fmt.Errorf("[%v] OpenFile(%v) failed - %e", ctxString, localFullPath, err)
	} else if blobEntry, err := lw.backend.Get(lw.relativePath); err != nil {
		return fmt.Errorf("[%v] backend.Get(%v) failed - %e", ctxString, lw.relativePath, err)
	} else if err = util.CopyAndClose(file, blobEntry.Content); err != nil {
		return fmt.Errorf("[%v] CopyAndClose(%v) failed - %e", ctxString, lw.relativePath, err)
	}
	return nil
}

func (lw *localWrite) String() string {
	return fmt.Sprintf("localWrite:%v", lw.relativePath)
}
