package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"

	"github.com/osga1291/upload/dataocean"
	"github.com/osga1291/upload/fileservice"
	"github.com/osga1291/upload/shared"
)

func CreateFSFile() {
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

func CreateDOFile() string {
	do := dataocean.NewDataOcean()

	payload := map[string]interface{}{
		"file": map[string]interface{}{
			"path":      fmt.Sprintf("/folderA/%s", shared.GenerateRandomString(5)),
			"regions":   []string{"us1"},
			"multipart": false,
			"fileset":   false,
		},
	}
	file, err := os.Open("/Users/ogandar/Desktop/upload_folder/input/profile001.gif")
	if err != nil {
		log.Panic(err)
	}

	fileId, err := shared.Upload(do, payload, file)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("File ID: %s\n", fileId)
	return fileId

}

func GetDoFile(fileId string, wg *sync.WaitGroup) {
	defer wg.Done()
	do := dataocean.NewDataOcean()

	resp, err := shared.GetFile(do, fileId)
	if err != nil {
		log.Panic(err)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Panic(err)
	}
	var result map[string]interface{}
	err = json.Unmarshal(bodyBytes, &result)
	if err != nil {
		log.Panic(err)
	}

	if resp.StatusCode == http.StatusOK {
		if file, ok := result["file"].(map[string]interface{}); ok {
			if regions, ok := file["regions"].([]interface{}); ok {
				if len(regions) != 0 {
					for _, region := range regions {
						if regionStr, ok := region.(string); ok && regionStr == "eu1" {
							fmt.Println("File is available: ", fileId)
							return

						}
					}
					log.Panic("File is not available: ", fileId)
				}
			} else {
				log.Panic("Empty regions: ", fileId)
			}
		}
	}
}

func WriteToCSV(wg *sync.WaitGroup, fileId string, writer *csv.Writer, mutex *sync.Mutex) {
	defer wg.Done()
	mutex.Lock()
	defer mutex.Unlock()

	err := writer.Write([]string{fileId})
	if err != nil {
		log.Panic(err)
	}

}
func ReadCSVFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var fileIds []string
	for _, record := range records[1:] { // Skip header
		fileIds = append(fileIds, record[0])
	}

	return fileIds, nil
}

func CheckFile() {
	// Read the CSV file and print the file IDs
	fileIds, err := ReadCSVFile("/Users/ogandar/Desktop/upload_folder/test18.csv")
	if err != nil {
		log.Panic(err)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	for _, fileId := range fileIds {
		wg.Add(1)
		sem <- struct{}{}
		go func(fileId string) {
			defer func() { <-sem }()
			GetDoFile(fileId, &wg)
		}(fileId)
	}
	wg.Wait()

}

func CreateFileToCSV() {
	csvFile, err := os.Create("/Users/ogandar/Desktop/upload_folder/test22.csv")
	if err != nil {
		log.Panic(err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	err = writer.Write([]string{"file_id"})
	if err != nil {
		log.Panic(err)
	}
	var wg sync.WaitGroup
	var mutex sync.Mutex
	sem := make(chan struct{}, 40)

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			fileId := CreateDOFile()
			WriteToCSV(&wg, fileId, writer, &mutex)
		}()
	}
	wg.Wait()
}

func main() {

	CreateFileToCSV()
	CheckFile()

	/*
		csvFile, err := os.Create("/Users/ogandar/Desktop/upload_folder/test1.csv")
		if err != nil {
			log.Panic(err)
		}
		defer csvFile.Close()

		writer := csv.NewWriter(csvFile)
		defer writer.Flush()

		err = writer.Write([]string{"FileID"})
		if err != nil {
			log.Panic(err)
		}
		var wg sync.WaitGroup
		var mutex sync.Mutex
		sem := make(chan struct{}, 10)

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer func() { <-sem }()
				fileId := CreateDOFile()
				WriteToCSV(&wg, fileId, writer, &mutex)
			}()
		}
		wg.Wait()
	*/

}
