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

const writerClientIdKey = "WriterClientId"

type MetaEntry struct {
	BasePath           string
	RelPath            util.RelPathType
	Md5                string // hex string of md5
	ModTime            time.Time
	BlobWriterClientId *string
}

type FullEntry struct {
	*MetaEntry
	// Who ever holds this should close the reader
	Content io.ReadCloser
}

type Backend interface {
	ListDirRecursive(prefix string) (map[util.RelPathType]MetaEntry, error)
	GetMeta(name util.RelPathType) (*MetaEntry, error)
	Delete(name util.RelPathType) error
	Get(name util.RelPathType) (*FullEntry, error)
	// Reader will be closed by Put
	Put(name util.RelPathType, reader io.ReadCloser) error
}

type GcpBackend struct {
	client     *gcs.Client
	bucket     *gcs.BucketHandle
	basePrefix string
	clientId   string
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
	g.clientId = util.UniqueMachineId
	fmt.Println(g.bucket, "--", g.basePrefix)
	return &g
}

func (g *GcpBackend) ListDirRecursive(prefix string) (map[util.RelPathType]MetaEntry, error) {
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
	ret := make(map[util.RelPathType]MetaEntry)
	for {
		next, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		entry := MetaEntry{
			BasePath: basePath,
			RelPath:  util.RelPathType(strings.TrimPrefix(next.Name, basePath)),
			Md5:      hex.EncodeToString(next.MD5),
			ModTime:  next.Updated,
		}
		writerClientId, ok := next.Metadata[writerClientIdKey]
		if ok {
			entry.BlobWriterClientId = &writerClientId
		}

		ret[entry.RelPath] = entry
	}
	return ret, nil
}

func (g *GcpBackend) Delete(name util.RelPathType) error {
	return g.bucket.Object(path.Join(g.basePrefix, name.String())).Delete(context.TODO())
}

func (g *GcpBackend) GetMeta(name util.RelPathType) (*MetaEntry, error) {
	attrs, err := g.bucket.Object(path.Join(g.basePrefix, name.String())).Attrs(context.TODO())
	if err != nil {
		return nil, err
	}
	ret := &MetaEntry{
		BasePath: g.basePrefix,
		RelPath:  name,
		Md5:      hex.EncodeToString(attrs.MD5),
		ModTime:  attrs.Updated,
	}
	writerClientId, ok := attrs.Metadata[writerClientIdKey]
	if ok {
		ret.BlobWriterClientId = &writerClientId
	}
	return ret, nil
}

func (g *GcpBackend) Get(name util.RelPathType) (*FullEntry, error) {
	o := g.bucket.Object(path.Join(g.basePrefix, name.String()))
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

func (g *GcpBackend) Put(name util.RelPathType, reader io.ReadCloser) error {
	o := g.bucket.Object(path.Join(g.basePrefix, name.String()))
	log.Printf("Writing to %v:%v", o.BucketName(), o.ObjectName())
	w := o.NewWriter(context.TODO())
	if w.ObjectAttrs.Metadata == nil {
		w.ObjectAttrs.Metadata = make(map[string]string)
	}
	w.ObjectAttrs.Metadata[writerClientIdKey] = g.clientId
	return util.CopyAndClose(w, reader)
}

func NewBackend(baseURL url.URL, _ string) Backend {
	if baseURL.Scheme != "gs" {
		panic("Wrong scheme" + baseURL.String())
	}
	return GcpBackend{}.Init(baseURL.Host, baseURL.Path)
}
