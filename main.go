package main

import (
	"flag"
	"github.com/dotslash/cloudsync/blob"
	"github.com/dotslash/cloudsync/syncer"
	"log"
	"net/url"
)

func main() {
	localPath := flag.String("local", ".", "Local Path")
	localTrash := flag.String("local_trash", "./.trash", "Local Trash")
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
	syncerObj := syncer.NewSyncer(*localPath, *localTrash, blobStore)
	syncerObj.Start()
}
