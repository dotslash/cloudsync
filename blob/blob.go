package blob

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/dotslash/cloudsync/util"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)
import gcs "cloud.google.com/go/storage"

type MetaEntry struct {
	BasePath string
	RelPath  string
	Md5      string // hex string of md5
	ModTime  time.Time
}

type FullEntry struct {
	*MetaEntry
	// Who ever holds this should close the reader
	Content io.ReadCloser
}

type Backend interface {
	ListDirRecursive(prefix string) ([]MetaEntry, error)
	GetMeta(name string) (*MetaEntry, error)
	Delete(name string) error
	Get(name string) (*FullEntry, error)
	// Reader will be closed by Put
	Put(name string, reader io.ReadCloser) error
}

type GcpBackend struct {
	client     *gcs.Client
	bucket     *gcs.BucketHandle
	basePrefix string
}

func (g GcpBackend) Init(bucket string, basePrefix string) *GcpBackend {
	var err error
	g.client, err = gcs.NewClient(
		context.TODO(), option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")))
	if err != nil {
		panic(fmt.Sprintf("Failed to create client %v", err))
	}
	g.bucket = g.client.Bucket(bucket)
	g.basePrefix = strings.Trim(basePrefix, "/")
	fmt.Println(g.bucket, "--", g.basePrefix)
	return &g
}

func (g *GcpBackend) ListDirRecursive(prefix string) ([]MetaEntry, error) {
	basePath := g.basePrefix + prefix
	if !strings.HasSuffix(basePath, "/") {
		basePath = basePath + "/"
	}
	if basePath == "/" {
		basePath = ""
	}
	it := g.bucket.Objects(context.TODO(), &gcs.Query{
		Prefix:     basePath,
		Versions:   false,
		Projection: gcs.ProjectionFull,
	})
	ret := make([]MetaEntry, 0)
	for {
		next, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		ret = append(ret, MetaEntry{
			BasePath: basePath,
			RelPath:  strings.TrimPrefix(next.Name, basePath),
			Md5:      hex.EncodeToString(next.MD5),
			ModTime:  next.Updated,
		})
	}
	return ret, nil
}

func (g *GcpBackend) Delete(name string) error {
	return g.bucket.Object(path.Join(g.basePrefix, name)).Delete(context.TODO())
}

func (g *GcpBackend) GetMeta(name string) (*MetaEntry, error) {
	attrs, err := g.bucket.Object(path.Join(g.basePrefix, name)).Attrs(context.TODO())
	if err != nil {
		return nil, err
	}
	return &MetaEntry{
		BasePath: g.basePrefix,
		RelPath:  name,
		Md5:      hex.EncodeToString(attrs.MD5),
		ModTime:  attrs.Updated,
	}, nil
}

func (g *GcpBackend) Get(name string) (*FullEntry, error) {
	o := g.bucket.Object(path.Join(g.basePrefix, name))
	if attrs, err := o.Attrs(context.TODO()); err != nil {
		return nil, err
	} else if reader, err := o.NewReader(context.TODO()); err != nil {
		return nil, err
	} else {
		return &FullEntry{
			MetaEntry: &MetaEntry{
				BasePath: g.basePrefix,
				RelPath:  name,
				Md5:      hex.EncodeToString(attrs.MD5),
				ModTime:  attrs.Updated,
			},
			Content: reader,
		}, nil
	}
}

func (g *GcpBackend) Put(name string, reader io.ReadCloser) error {
	o := g.bucket.Object(path.Join(g.basePrefix, name))
	log.Printf("Writing to %v:%v", o.BucketName(), o.ObjectName())
	w := o.NewWriter(context.TODO())
	return util.CopyAndClose(w, reader)
}

func NewBackend(baseURL url.URL, _ string) Backend {
	if baseURL.Scheme != "gs" {
		panic("Wrong scheme" + baseURL.String())
	}
	return GcpBackend{}.Init(baseURL.Host, baseURL.Path)
}
