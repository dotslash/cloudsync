package main

import (
	"fmt"
	"github.com/akamensky/argparse"
	"github.com/dotslash/cloudsync/blob"
	"github.com/dotslash/cloudsync/util"
	"net/url"
	"os"
	"strings"
)

func debugBlob(args []string) {
	println(strings.Join(args, "\n"))
	u, _ := url.Parse(args[0])
	backend := blob.NewBackend(*u, "")
	entries, err := backend.ListDirRecursive("")
	if err != nil {
		panic(fmt.Sprintf("ok %v", err))
	}
	for _, e := range entries {
		md5Str := e.Md5
		fmt.Printf("%v %v %v %v\n", e.BasePath, e.RelPath, e.ModTime.String(), md5Str)
	}
}
func main() {
	if os.Args[1] == "blob" {
		debugBlob(os.Args[2:])
	} else if os.Args[1] == "file" {
		debugFile(os.Args[1:])
	} else if os.Args[1] == "blob-get" {
		u, _ := url.Parse(os.Args[2])
		backend := blob.NewBackend(*u, "")
		meta, _ := backend.GetMeta(util.RelPathType(os.Args[3]))
		fmt.Println(meta)
	} else {
		id, err := util.GetUniqueMachineId()
		fmt.Println("Error: ", err)
		fmt.Println("uniqueId", id)
	}
}

func debugFile(args []string) {
	parser := argparse.NewParser("debug_file", "debug file")
	path := parser.String("p", "path", &argparse.Options{Required: true, Help: "Path"})
	if err := parser.Parse(args); err != nil {
		fmt.Print(parser.Usage(err))
	}
	res, err := util.ListFilesRec(*path)
	util.PanicIfErr(err, "ListFilesRec failed")
	for _, meta := range res {
		fmt.Printf("%v %v %v\n", meta.BaseDir, meta.RelPath, meta.Md5sum)
	}
}
