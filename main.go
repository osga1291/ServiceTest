package main

import (
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"

	"github.com/osga1291/upload/fileservice"
	"github.com/osga1291/upload/shared"
)

func main() {
	// Create a new FileService
	fs := fileservice.NewFileService()

	fs.CacheSpace("cebd37fd-a802-41fa-8994-ea03b27dafcb")

	// Create a new file
	payload := map[string]interface{}{
		"name":      "test2.txt",
		"parentId":  "dd0475ec-a2b2-4d9c-b682-b799ccc823cb",
		"multipart": true,
	}
	file, err := os.Open("/Users/ogandar/Downloads/temp.zip")
	if err != nil {
		log.Panic(err)
	}
	fileId, err := shared.Upload(fs, payload, file)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("File ID: %s\n", fileId)

}
