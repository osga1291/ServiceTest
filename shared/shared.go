package shared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"math/rand"
)

var (
	bearerToken string
	bearerMutex sync.Mutex
)

var mutex = sync.Mutex{}

type NonBlocking struct {
	Response   *http.Response
	Error      error
	PartNumber int
}
type AssembleTag struct {
	Etag       string `json:"-"`
	PartNumber int    `json:"-"`
	EtagTag    string `json:"-"`
	PartTag    string `json:"-"`
}

func (a *AssembleTag) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		a.EtagTag: a.Etag,
		a.PartTag: a.PartNumber,
	})
}

type ChunkData struct {
	PartNumber int
	Chunk      []byte
}

type UploadOptions struct {
	MaxRoutines   int
	ChunkSize     int64
	ContentLength int64
}

type UploadStruct struct {
	arr []byte
}

func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func getFileSize(file *os.File) (int64, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return fileInfo.Size(), nil
}

func GenerateRandomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func fetchBearerToken(client *http.Client) (string, error) {
	var req *http.Request
	var err error
	payload := strings.NewReader("grant_type=client_credentials&scope=oscar-test")
	req, err = http.NewRequest("POST", "https://stage.id.trimblecloud.com/oauth/token", payload)
	if err != nil {
		return "", err

	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth("a46ca581-f699-4b3a-a2c2-0f177015d599", "392d56c82d4e4458a3e6547b4c136db9")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	if resp.StatusCode == http.StatusOK {
		var result map[string]interface{}
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		err = json.Unmarshal(bodyBytes, &result)
		if err != nil {
			return "", err
		}
		return result["access_token"].(string), nil

	}

	return "", fmt.Errorf("Request failed with status: %d", resp.StatusCode)

}

func Request(client *http.Client, action string, baseUrl string, body *[]byte, queryParams map[string]string) (*http.Response, error) {
	var req *http.Request
	var err error

	// Parse the base URL
	parsedURL, err := url.Parse(baseUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	// Add query parameters to the URL
	if queryParams != nil {
		q := parsedURL.Query()
		for key, value := range queryParams {
			q.Add(key, value)
		}
		parsedURL.RawQuery = q.Encode()
	}

	if action == "GET" {
		req, err = http.NewRequest(action, parsedURL.String(), nil)
	} else {
		req, err = http.NewRequest(action, parsedURL.String(), bytes.NewReader(*body))
	}
	if err != nil {
		return nil, err

	}
	if action != "PUT" {
		req.Header.Set("Content-Type", "application/json")
		var bearer = "Bearer " + bearerToken
		req.Header.Add("Authorization", bearer)
	}

	resp, err := client.Do(req)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		if resp.StatusCode == http.StatusUnauthorized {
			mutex.Lock()
			bearerToken, err = fetchBearerToken(client)
			mutex.Unlock()
			if err != nil {
				return nil, err
			}
			return Request(client, action, baseUrl, body, queryParams)
		}
		var result map[string]interface{}
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(bodyBytes, &result)
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("Request failed with status: %d, response: %s", resp.StatusCode, string(bodyBytes))
	}
	return resp, nil
}

func defaultUploadOptions(file *os.File, options ...UploadOptions) (UploadOptions, error) {
	opts := UploadOptions{
		MaxRoutines:   2 * runtime.NumCPU(),
		ChunkSize:     50 * 1024 * 1024, // 50 MB
		ContentLength: 0,
	}
	if len(options) > 0 {
		opts = options[0]
	}

	if opts.ContentLength == 0 {
		contentLength, err := getFileSize(file)
		if err != nil {
			return UploadOptions{}, err
		}
		opts.ContentLength = contentLength
	}
	return opts, nil
}

func Upload(service Service, payload map[string]interface{}, queryParams map[string]string, file *os.File, options ...UploadOptions) (string, error) {
	opts, err := defaultUploadOptions(file, options...)
	if err != nil {
		return "", err
	}

	isMulti, error := service.CheckIfMultipart(payload)
	if error != nil {
		return "", error
	}

	if !isMulti {
		if opts.ContentLength < opts.ChunkSize {
			return SinglepartUpload(service, payload, queryParams, file, opts)
		} else {
			return "", fmt.Errorf("content length is greater than chunk size")
		}
	} else {
		return MultipartUpload(service, payload, queryParams, file, opts)
	}
}

func CreateFile(service Service, payload map[string]interface{}, url string, queryParams map[string]string) (*http.Response, error) {

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		log.Panic(err)
	}

	resp, err := Request(service.GetClient(), "POST", url, &jsonBytes, queryParams)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("JSON request failed with status: %d", resp.StatusCode)
	}
	return resp, nil
}

func GetFile(service Service, fileId string, queryParams map[string]string) (*http.Response, error) {

	url, err := service.GetUrl("getFile", map[string]string{"fileId": fileId})
	if err != nil {
		return nil, err
	}

	resp, err := Request(service.GetClient(), "GET", url, nil, queryParams)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JSON request failed with status: %d", resp.StatusCode)
	}
	return resp, nil

}

func SinglepartUpload(service Service, payload map[string]interface{}, queryParams map[string]string, file *os.File, options ...UploadOptions) (string, error) {
	opts, err := defaultUploadOptions(file, options...)
	if err != nil {
		return "", err
	}
	var startIndex int64 = 0
	var endIndex int64 = Min(startIndex+opts.ChunkSize, opts.ContentLength)
	b1 := make([]byte, endIndex-startIndex)

	_, err = file.ReadAt(b1, startIndex)
	if err != nil {
		return "", err
	}

	url, err := service.GetUrl("createFile", nil)
	if err != nil {
		return "", err
	}

	resp, err := CreateFile(service, payload, url, queryParams)
	if err != nil {
		return "", err
	}

	id, url, fileId, err := service.ExtractCreateFileResp(resp)
	if err != nil {
		return "", err
	}
	_, err = Request(service.GetClient(), "PUT", url, &b1, queryParams)

	if err != nil {
		return "", err
	}

	err = service.WaitForAvailable(id)
	if err != nil {
		return "", err
	}
	return fileId, nil
}

func MultipartUpload(service Service, payload map[string]interface{}, queryParams map[string]string, file *os.File, options ...UploadOptions) (string, error) {
	opts, err := defaultUploadOptions(file, options...)
	if err != nil {
		return "", err
	}
	url, err := service.GetUrl("createFile", nil)
	if err != nil {
		return "", err
	}
	resp, err := CreateFile(service, payload, url, queryParams)
	if err != nil {
		return "", err
	}
	id, url, fileId, err := service.ExtractCreateFileResp(resp)

	if err != nil {
		return "", err
	}

	c := make(chan NonBlocking)
	nb := []AssembleTag{}
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		handleUpload(service, c, &nb)
	}()

	go func() {
		download(service.GetClient(), c, file, &wg, opts, url)

	}()

	wg.Wait()
	close(c)

	err = service.Assemble(id, nb)
	if err != nil {
		return "", err
	}

	err = service.WaitForAvailable(id)
	if err != nil {
		return "", err
	}

	return fileId, nil

}

func download(client *http.Client, c chan NonBlocking, file *os.File, wg *sync.WaitGroup, options UploadOptions, url string) error {
	defer wg.Done()
	var parts int = int(options.ContentLength/options.ChunkSize) + 1
	if parts > 1000 {
		return fmt.Errorf("number of parts is greater than 1000 update chunk size")
	}
	fmt.Printf("Parts %d\n", parts)
	// Channel for chunks with part numbers
	chunks := make(chan ChunkData, options.MaxRoutines)

	// Start upload workers
	uploadWg := &sync.WaitGroup{}
	for i := 0; i < options.MaxRoutines; i++ {
		uploadWg.Add(1)
		go func() {
			defer uploadWg.Done()
			upload(client, c, chunks, url)
		}()
	}

	// Read file chunks and send them to the channel
	for i := 1; i <= parts; i++ {
		startIndex := int64(i-1) * options.ChunkSize
		endIndex := Min(startIndex+options.ChunkSize, options.ContentLength)

		// Read the file chunk
		b1 := make([]byte, endIndex-startIndex)
		mutex.Lock()
		_, err := file.ReadAt(b1, startIndex)
		mutex.Unlock()
		if err != nil {
			log.Panic(err)
		}

		// Send a deep copy of the chunk with its part number
		chunks <- ChunkData{
			PartNumber: i,
			Chunk:      append([]byte(nil), b1...), // Deep copy
		}
	}

	// Close the chunk channel after all parts have been sent
	close(chunks)

	// Wait for all uploads to complete
	uploadWg.Wait()
	return nil
}

func upload(client *http.Client, c chan NonBlocking, chunks chan ChunkData, url string) {
	for chunk := range chunks {
		if len(chunk.Chunk) == 0 {
			log.Panic("Empty chunk")
		}
		mutex.Lock()
		copyChunk := append([]byte(nil), chunk.Chunk...)
		resp, err := Request(client, "PUT", strings.Replace(url, "*", strconv.Itoa(chunk.PartNumber), -1), &copyChunk, nil)
		mutex.Unlock()
		if err != nil {
			log.Panic(err)
		}

		fmt.Printf("Request solution: %d\n", resp.StatusCode)

		if resp.StatusCode == http.StatusOK {
			fmt.Printf("Request: %d\n", chunk.PartNumber)
		}

		c <- NonBlocking{
			Response:   resp,
			Error:      err,
			PartNumber: chunk.PartNumber,
		}
	}
}

func handleUpload(s Service, c chan NonBlocking, nb *[]AssembleTag) error {
	for resp := range c {
		if resp.Error != nil {
			return resp.Error
		} else {
			if resp.Response.StatusCode == http.StatusOK {
				etag := resp.Response.Header["Etag"][0]
				json.Unmarshal([]byte(etag), &etag)
				fmt.Printf("Etag %s, part_number %d", etag, resp.PartNumber)
				x := s.CreateTag(etag, resp.PartNumber)
				mutex.Lock()
				*nb = append(*nb, x)
				mutex.Unlock()
			} else {
				return resp.Error
			}
		}
	}
	return nil
}
