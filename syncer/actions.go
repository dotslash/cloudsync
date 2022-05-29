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
}

func (lw *localWrite) do() error {
	localFullPath := path.Join(lw.localBasePath, lw.relativePath)
	log.Printf("Writing from remote:%v to %v", lw.relativePath, localFullPath)
	if err := os.MkdirAll(path.Dir(localFullPath), 0755); err != nil {
		return fmt.Errorf("localToWrite: MkdirAll(%v) failed - %e", path.Dir(localFullPath), err)
	}
	if file, err := os.OpenFile(localFullPath, os.O_CREATE|os.O_RDWR, 0755); err != nil {
		return fmt.Errorf("localToWrite: OpenFile(%v) failed - %e", localFullPath, err)
	} else if blobEntry, err := lw.backend.Get(lw.relativePath); err != nil {
		return fmt.Errorf("localToWrite: backend.Get(%v) failed - %e", lw.relativePath, err)
	} else if err = util.CopyAndClose(file, blobEntry.Content); err != nil {
		return fmt.Errorf("localToWrite: CopyAndClose(%v) failed - %e", lw.relativePath, err)
	}
	return nil
}

func (lw *localWrite) String() string {
	return fmt.Sprintf("blobWrite:%v", lw.relativePath)
}
