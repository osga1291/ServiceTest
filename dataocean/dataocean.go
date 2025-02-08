package dataocean

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/osga1291/upload/shared"
)

type DataOcean struct {
	client http.Client
	path   string
	urls   map[string]string
}

type AssemblyPage struct {
	Assembly AssemblyParts `json:"upload"`
}

type AssemblyParts struct {
	Etags []shared.AssembleTag `json:"parts"`
}

type AssembleTag struct {
	Etag       string `json:"etag"`
	PartNumber int    `json:"part_number"`
}

func NewDataOcean() *DataOcean {
	return &DataOcean{
		client: http.Client{},
		urls: map[string]string{
			"createFolder": "*/folders",
			"createFile":   "*/files",
			"getFile":      "*/files/fileId",
			"assembleFile": "*/files/resourceId/assemble",
		},
	}
}

func (do *DataOcean) GetClient() *http.Client {
	return &do.client
}

func (do *DataOcean) GetUrl(action string, replaceMap map[string]string) (string, error) {
	url, ok := do.urls[action]
	if !ok {
		return "", fmt.Errorf("action %s is not found", action)
	}

	for k, v := range replaceMap {
		url = strings.Replace(url, k, v, -1)
	}
	return url, nil

}

func (do *DataOcean) ExtractCreateFileResp(resp *http.Response) (string, string, string, error) {
	var result map[string]interface{}
	defer resp.Body.Close()
	if resp.Body != nil {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Panic(err)
		}
		err = json.Unmarshal(bodyBytes, &result)
		if err != nil {
			log.Panic(err)
		}
		if file, ok := result["file"].(map[string]interface{}); ok {
			upload, ok := file["upload"].(map[string]interface{})
			if ok {
				return file["id"].(string), upload["url"].(string), file["id"].(string), nil
			}
		}
	}
	return "", "", "", fmt.Errorf("Error")
}

func (do *DataOcean) WaitForAvailable(resourceId string) error {
	url, err := do.GetUrl("getFile", map[string]string{"fileId": resourceId})
	if err != nil {
		return err
	}
	for {
		time.Sleep(2 * time.Second)
		resp, err := shared.Request(
			do.GetClient(), "GET", url, nil)

		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.Body != nil {
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Panic(err)
			}
			var result map[string]interface{}
			err = json.Unmarshal(bodyBytes, &result)
			if err != nil {
				return err
			}
			if resp.StatusCode == http.StatusOK {
				if file, ok := result["file"].(map[string]interface{}); ok {
					if status, ok := file["status"]; ok && status == "AVAILABLE" {
						fmt.Println("File is available")
						return nil
					} else if status == "ARCHIVE_PROCESSING_FAILED" {
						return fmt.Errorf("File processing failed")
					}
				}
			} else {
				return fmt.Errorf("Bad request on waiting for file: %s", result)
			}
		}
	}
}

func (do *DataOcean) Assemble(id string, parts []shared.AssembleTag) error {

	p := AssemblyParts{
		Etags: parts,
	}

	page := AssemblyPage{
		Assembly: p,
	}

	s, err := json.Marshal(page)
	if err != nil {
		return err
	}
	fmt.Println(string(s))
	url, err := do.GetUrl("assembleFile", map[string]string{"resourceId": id})
	if err != nil {
		return err
	}

	resp, err := shared.Request(do.GetClient(), "POST", url, &s)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Assemble request failed with status: %d\n", resp.StatusCode)
	}
	return nil

}

func (do *DataOcean) CreateTag(etag string, partNumber int) shared.AssembleTag {
	return shared.AssembleTag{
		Etag:       etag,
		PartNumber: partNumber,
		EtagTag:    "etag",
		PartTag:    "part_number",
	}
}

func (do *DataOcean) rename(fileId string) string {
	randPath := fmt.Sprintf("/%s/updated/name", shared.GenerateRandomString(10))
	jsonData := map[string]interface{}{
		"file": map[string]string{
			"path": randPath,
		},
	}
	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		log.Panic(err)
	}
	resp, err := shared.Request(do.GetClient(), "PATCH", fmt.Sprintf("*/files/%s", fileId), &jsonBytes)
	if resp.StatusCode == http.StatusAccepted {
		fmt.Println("JSON request successful")
	} else {
		fmt.Printf("JSON request failed with status: %d\n", resp.StatusCode)
	}

	return randPath
}

func (do *DataOcean) CheckIfMultipart(payload map[string]interface{}) (bool, error) {

	if file, ok := payload["file"].(map[string]interface{}); ok {
		if multipart, ok := file["multipart"]; !ok || multipart.(bool) == false {
			return false, nil
		} else {
			return true, nil
		}
	}
	return false, fmt.Errorf("Invalid payload")
}
