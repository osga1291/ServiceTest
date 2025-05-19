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

func CreateFSFile(parentId string) {
	// Create a new FileService
	fs := fileservice.NewFileService()

	fs.CacheSpace("66e654cf-67cb-4b44-ba7d-4981bbe7257e")

	// Create a new file
	payload := map[string]interface{}{
		"name":      shared.GenerateRandomString(5),
		"parentId":  parentId,
		"multipart": true,
	}

	queryParams := map[string]string{
		"urlDuration": "7d",
	}

	file, err := os.Open("/Users/ogandar/Downloads/big_file_test/connect_big_file")
	if err != nil {
		log.Panic(err)
	}
	fileId, err := shared.Upload(fs, payload, queryParams, file)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("File ID: %s\n", fileId)
}
func CreateFSFolder(parentId string) string {
	// Create a new FileService
	fs := fileservice.NewFileService()

	fs.CacheSpace("66e654cf-67cb-4b44-ba7d-4981bbe7257e")

	id, err := fs.CreateFolder(parentId)
	if err != nil {
		log.Panic(err)
	}
	return id
}

func CreateDOSupportFile() {
	do := dataocean.NewDataOcean()

	payload := map[string]interface{}{
		"file": map[string]interface{}{
			"path":      fmt.Sprintf("/%s/REPRESENTATION/fileVersion123/%s.png", shared.GenerateRandomString(5), shared.GenerateRandomString(5)),
			"regions":   []string{"us1"},
			"multipart": false,
			"fileset":   false,
		},
	}
	file, err := os.Open("/Users/ogandar/Downloads/temp.zip")
	if err != nil {
		log.Panic(err)
	}

	fileId, err := shared.Upload(do, payload, nil, file)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("File ID: %s\n", fileId)
}

func CreateDOFile(filePath string, singlepart bool) string {
	do := dataocean.NewDataOcean()

	payload := map[string]interface{}{
		"file": map[string]interface{}{
			"path":      fmt.Sprintf("/folderA/%s", shared.GenerateRandomString(5)),
			"regions":   []string{"us1"},
			"multipart": !singlepart,
			"fileset":   false,
		},
	}
	file, err := os.Open(filePath)
	if err != nil {
		log.Panic(err)
	}

	fileId, err := shared.Upload(do, payload, nil, file)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("File ID: %s\n", fileId)
	return fileId

}

func GetDoFile(fileId string, wg *sync.WaitGroup) {
	defer wg.Done()
	do := dataocean.NewDataOcean()

	resp, err := shared.GetFile(do, fileId, nil)
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
	fileIds, err := ReadCSVFile("/Users/ogandar/Desktop/upload_folder/mount/output/zTknw.csv")
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

func CreateFileToCSV(numberFiles int, filePath string, singlePart bool) {

	name := shared.GenerateRandomString(5)

	csvFile, err := os.Create(fmt.Sprintf("/Users/ogandar/Desktop/upload_folder/mount/output/%s.csv", name))
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
	//var mutex sync.Mutex

	sem := make(chan struct{}, 40)
	if !singlePart {
		sem = make(chan struct{}, 5)
	}
	parentId := "5de170a9-ebb5-42d8-87c1-35bc0594c914"
	for i := 0; i < numberFiles; i++ {
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			parentId = CreateFSFolder(parentId)
			CreateFSFile(parentId)
			//WriteToCSV(&wg, fileId, writer, &mutex)
		}()
	}
	wg.Wait()
	fmt.Printf("File created: %s\n", name)
}

func main() {
	CreateFSFile("5de170a9-ebb5-42d8-87c1-35bc0594c914")
	//CreateDOFile("/Users/ogandar/Desktop/upload_folder/mount/input/VSCode-darwin-universal.zip", false)
	//CreateFileToCSV(1000, "/Users/ogandar/Desktop/upload_folder/mount/input/route53.zip", true)
	//CreateDOFile(100, "/Users/ogandar/Desktop/upload_folder/mount/input/route53.zip", true)
	//CheckFile()

}

/*
func main() {


	if len(os.Args) < 4 {
		log.Fatalf("Usage: %s <numberFiles> <filePath> <singlePart>", os.Args[0])
	}

	numberFiles, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid numberFiles: %v", err)
	}

	filePath := os.Args[2]
	singlePart, err := strconv.ParseBool(os.Args[3])
	if err != nil {
		log.Fatalf("Invalid singlePart: %v", err)
	}

	CreateFileToCSV(numberFiles, filePath, singlePart)

}
*/
