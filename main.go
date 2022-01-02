package main

import (
	"flag"
	"github.com/dotslash/cloudsync/blob"
	"log"
	"net/url"
)

func main() {
	localPath := flag.String("local", ".", "Local Path")
	//initialTruth := flag.String("initial_truth", "local",
	//	"Initial source of truth. When the program starts if local and "+
	//		"remote disagree initial_truth will will")
	remotePath := flag.String("remote", "",
		"Local Path (currently only gcp is supported)",
	)
	remoteTrash := flag.String("remote_trash_prefix", ".trash",
		"Removed blobs will be stored in this prefix. Items in trash whose "+
			"timestamp is older than 30 days will be deleted for good")
	flag.Parse()
	if *remotePath == "" {
		log.Fatalln("Oops: remotePath is empty")
	}
	remote, _ := url.Parse(*remotePath)
	blobStore := blob.NewBackend(*remote, *remoteTrash)
	// localDataIsInitialTruth := *initialTruth == "local"
	syncer := newSyncer(*localPath, blobStore)
	syncer.start()
}
