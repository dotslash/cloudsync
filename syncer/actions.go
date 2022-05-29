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
	relativeFilePath util.RelPathType
}

func (lr *localRemove) do() error {
	fullPath := path.Join(lr.basePath, lr.relativeFilePath.String())
	log.Printf("localRemove(%v): full path:%v", lr.relativeFilePath, fullPath)
	return os.Remove(fullPath)
}

func (lr *localRemove) String() string {
	return fmt.Sprintf("localRemove(%v)", lr.relativeFilePath)
}

type blobRemove struct {
	relativeFilePath util.RelPathType
	backend          blob.Backend
}

func (s *blobRemove) do() error {
	log.Printf("blobRemove(%v): Removing %v", s.relativeFilePath, s.relativeFilePath)
	return s.backend.Delete(s.relativeFilePath)
}

func (br *blobRemove) String() string {
	return fmt.Sprintf("blobRemove(%v)", br.relativeFilePath)
}

type blobWrite struct {
	localBasePath string
	relativePath  util.RelPathType
	backend       blob.Backend
	localMeta     *util.LocalFileMeta
	remoteMeta    *blob.MetaEntry
}

func (bw *blobWrite) do() error {
	ctxString := fmt.Sprintf("blobWrite(%v)", bw.relativePath)
	if bw.remoteMeta != nil && bw.localMeta != nil && bw.remoteMeta.Md5 == bw.localMeta.Md5sum {
		log.Printf("[%v] Skipping because md5 hashes already match", ctxString)
		return nil
	}
	localFullPath := path.Join(bw.localBasePath, bw.relativePath.String())
	log.Printf("[%v] Writing from %v to remote:%v", ctxString, localFullPath, bw.relativePath)
	file, err := os.Open(localFullPath)
	if err != nil {
		return fmt.Errorf("[%v] remoteToWrite: Open(%v %v) failed - %e", ctxString, localFullPath, bw.relativePath, err)
	}
	if err = bw.backend.Put(bw.relativePath, file); err != nil {
		return fmt.Errorf("[%v] remoteToWrite: Put(%v) failed - %e", ctxString, bw.relativePath, err)
	}
	return nil
}
func (bw *blobWrite) String() string {
	return fmt.Sprintf("blobWrite(%v)", bw.relativePath)
}

type localWrite struct {
	localBasePath string
	relativePath  util.RelPathType
	backend       blob.Backend
	blobInfo      *blob.MetaEntry
}

// TODO: Dont do the write if the target file exists already and has a higher timestamp
func (lw *localWrite) do() error {
	localFullPath := path.Join(lw.localBasePath, lw.relativePath.String())
	ctxString := fmt.Sprintf("localWrite(%v)", lw.relativePath)
	log.Printf("[%v] Starting remote:%v to %v", ctxString, lw.relativePath, localFullPath)
	// TODO: maybe handle error. Here i only care about the case where info is ready
	info, err := util.GetLocalFileMeta(lw.localBasePath, lw.relativePath.String())
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
	return fmt.Sprintf("localWrite(%v)", lw.relativePath)
}
