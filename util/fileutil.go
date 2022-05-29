package util

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"
)
import "path/filepath"

type LocalFileMeta struct {
	BaseDir string
	RelPath string
	ModTime time.Time
	Md5sum  string // hex string of md5 hash
}

func GetLocalFileMeta(basePath, relPath string) (*LocalFileMeta, error) {
	fullpath := path.Join(basePath, relPath)
	if info, err := os.Stat(fullpath); err != nil {
		return nil, err
	} else {
		return makeLocalFileMeta(basePath, fullpath, info)
	}
}

// TODO: this method takes basePath and fullPath. This is bazzare. Fix it.
func makeLocalFileMeta(basePath, path string, info fs.FileInfo) (*LocalFileMeta, error) {
	PanicIf(
		strings.HasSuffix(basePath, "/"),
		fmt.Sprintf("Base path should not end with /: %v", basePath),
	)
	PanicIfFalse(
		strings.HasPrefix(path, "/"),
		fmt.Sprintf("path should start with /: %v", path),
	)

	hasher := md5.New()
	if file, err := os.Open(path); err != nil {
		return nil, err
	} else if _, err = io.Copy(hasher, file); err != nil {
		return nil, err
	}
	return &LocalFileMeta{
		BaseDir: basePath,
		RelPath: strings.TrimPrefix(path, basePath+"/"),
		ModTime: info.ModTime(),
		Md5sum:  hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func ListFilesRec(basePath string) (ret []LocalFileMeta, err error) {
	PanicIfFalse(
		strings.HasPrefix(basePath, "/") && !strings.HasSuffix(basePath, "/"),
		fmt.Sprintf("Base path must begin with / and must not end with /: %v", basePath),
	)

	err = filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if info, err := d.Info(); err != nil {
			return err
		} else if info.IsDir() {
			return nil
		} else if meta, err := makeLocalFileMeta(basePath, path, info); err != nil {
			return err
		} else {
			ret = append(ret, *meta)
			return nil
		}
	})
	return ret, err
}

// created this so that we can use defer for the reader, writer in for loops
func CopyAndClose(to io.WriteCloser, from io.ReadCloser) error {
	defer to.Close()
	defer from.Close()
	_, err := io.Copy(to, from)
	return err
}
