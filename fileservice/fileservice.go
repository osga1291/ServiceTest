package fileservice

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/osga1291/upload/shared"
)

type FileService struct {
	cacheSpaceId string
	client       http.Client
	urls         map[string]string
}

type AssemblyPage struct {
	Etags []shared.AssembleTag `json:"parts"`
}

func NewFileService() *FileService {
	return &FileService{
		client: http.Client{},
		urls: map[string]string{
			"createSpace":  "*/spaces/spaceId?complete=True",
			"createFolder": "*/spaces/spaceId/folders?complete=True",
			"createFile":   "*/spaces/spaceId/uploads?complete=True",
			"getFile":      "*/spaces/spaceId/files/fileId?complete=true&status=active",
			"getUpload":    "*/spaces/spaceId/uploads/uploadId?complete=True",
			"assembleFile": "*/spaces/spaceId/uploads/resourceId?complete=True",
		},
	}
}

func (fs *FileService) GetClient() *http.Client {
	return &fs.client
}

// GetUrls returns the URLs map
func (fs *FileService) GetUrl(action string, replaceMap map[string]string) (string, error) {
	if replaceMap == nil {
		replaceMap = make(map[string]string)
	}
	_, ok := replaceMap["spaceId"]
	if !ok {
		if fs.cacheSpaceId == "" {
			return "", fmt.Errorf("cacheSpaceId is empty and spaceId is not provided")
		}
		replaceMap["spaceId"] = fs.cacheSpaceId
	}

	url, ok := fs.urls[action]
	if !ok {
		return "", fmt.Errorf("action %s is not found", action)
	}

	for k, v := range replaceMap {
		url = strings.Replace(url, k, v, -1)
	}
	return url, nil

}

func (fs *FileService) CacheSpace(id string) {
	fs.cacheSpaceId = id
}

func (fs *FileService) createFolder(parentId string) {
	jsonData := map[string]interface{}{
		"parentId": parentId,
		"name":     shared.GenerateRandomString(5),
	}
	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		log.Panic(err)
	}
	resp, err := shared.Request(fs.GetClient(), "POST", "*/spaces/a64218bc-8538-43ca-9d6d-e845a775722a/folders?complete=True", &jsonBytes)

	if resp.StatusCode == http.StatusCreated {
		fmt.Println("JSON request successful")
	} else {
		var result map[string]interface{}
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(bodyBytes, &result)
		fmt.Printf("JSON request failed with status: %s\n", bodyBytes)
	}
}

func (fs *FileService) createSpace() *http.Response {
	jsonData := map[string]interface{}{
		"space": map[string]interface{}{
			"name":      "test-space",
			"provider":  "aws",
			"accountId": "123456789",
			"acl": map[string]interface{}{
				"ContentManager": []string{"trn:tid:application:a46ca581-f699-4b3a-a2c2-0f177015d599"},
			},
		},
	}
	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		log.Panic(err)
	}
	resp, err := shared.Request(fs.GetClient(), "POST", "*/spaces?complete=True", &jsonBytes)

	if resp.StatusCode == http.StatusCreated {
		fmt.Println("JSON request successful")
	} else {
		var result map[string]interface{}
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(bodyBytes, &result)
		fmt.Printf("JSON request failed with status: %s\n", bodyBytes)
	}
	return resp
}

func (*FileService) ExtractCreateFileResp(resp *http.Response) (string, string, string, error) {
	var result map[string]interface{}
	defer resp.Body.Close()
	if resp.Body != nil {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", "", "", err
		}
		err = json.Unmarshal(bodyBytes, &result)
		if err != nil {
			return "", "", "", err
		}
		if fileInput, ok := result["fileInputUploadDetails"].(map[string]interface{}); ok {
			uploadUrl, ok := fileInput["upload"].(map[string]interface{})
			if ok {
				fmt.Printf("File upload Id: %s  and file Id: %s\n", result["id"], fileInput["fileId"])
				return result["id"].(string), uploadUrl["url"].(string), fileInput["fileId"].(string), nil
			}
		}
	}
	return "", "", "", fmt.Errorf("Response body is nil")
}

func (fs *FileService) WaitForAvailable(resourceId string) error {
	url, err := fs.GetUrl("getUpload", map[string]string{"uploadId": resourceId})
	if err != nil {
		return err
	}
	for {
		resp, err := shared.Request(
			fs.GetClient(), "GET", url, nil)

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
				if result, ok := result["result"].(map[string]interface{}); ok {
					if status, ok := result["status"]; ok && status == "COMPLETED" {
						fmt.Println("File is available")
						return nil
					}
				}
			} else {
				return fmt.Errorf("Bad request on waiting for file: %d", resp.StatusCode)
			}
		}
	}
}

func (fs *FileService) Assemble(id string, parts []shared.AssembleTag) error {

	page := AssemblyPage{
		Etags: parts,
	}

	s, err := json.Marshal(page)

	if err != nil {
		return err
	}
	fmt.Println(string(s))
	url, err := fs.GetUrl("assembleFile", map[string]string{"resourceId": id})
	if err != nil {
		return err
	}

	resp, err := shared.Request(fs.GetClient(), "PATCH", url, &s)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Assemble request failed with status: %d\n", resp.StatusCode)
	}
	return nil

}

func (fs *FileService) CreateTag(etag string, partNumber int) shared.AssembleTag {
	return shared.AssembleTag{
		Etag:       etag,
		PartNumber: partNumber,
		EtagTag:    "etag",
		PartTag:    "partNumber",
	}
}

func (do *FileService) CheckIfMultipart(payload map[string]interface{}) (bool, error) {
	if multipart, ok := payload["multipart"]; !ok || multipart.(bool) == false {
		return false, nil
	} else {
		return true, nil
	}
}
