package shared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/rand"
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

type UploadOptions struct {
	MaxRoutines   int
	ChunkSize     int64
	ContentLength int64
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
	rand.Seed(uint64(time.Now().UnixNano()))
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func Request(client *http.Client, action string, url string, body *[]byte) (*http.Response, error) {
	var req *http.Request
	var err error
	if action == "GET" {
		req, err = http.NewRequest(action, url, nil)
	} else {
		req, err = http.NewRequest(action, url, bytes.NewReader(*body))
	}
	if err != nil {
		return nil, err

	}
	if action != "PUT" {
		req.Header.Set("Content-Type", "application/json")
		var bearer = "Bearer " + ""
		req.Header.Add("Authorization", bearer)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		var result map[string]interface{}
		bodyBytes, err := io.ReadAll(resp.Body)
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
		MaxRoutines:   10,
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

func Upload(service Service, payload map[string]interface{}, file *os.File, options ...UploadOptions) (string, error) {
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
			return SinglepartUpload(service, payload, file, opts)
		} else {
			return "", fmt.Errorf("Content length is greater than chunk size")
		}
	} else {
		return MultipartUpload(service, payload, file, opts)
	}
}

func CreateFile(service Service, payload map[string]interface{}, url string) (*http.Response, error) {

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		log.Panic(err)
	}
	resp, err := Request(service.GetClient(), "POST", url, &jsonBytes)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("JSON request failed with status: %d", resp.StatusCode)
	}
	return resp, nil
}

func GetFile(service Service, fileId string) (*http.Response, error) {

	url, err := service.GetUrl("getFile", map[string]string{"fileId": fileId})
	if err != nil {
		return nil, err
	}

	resp, err := Request(service.GetClient(), "GET", url, nil)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JSON request failed with status: %d", resp.StatusCode)
	}
	return resp, nil

}

func SinglepartUpload(service Service, payload map[string]interface{}, file *os.File, options ...UploadOptions) (string, error) {
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

	resp, err := CreateFile(service, payload, url)
	if err != nil {
		return "", err
	}

	id, url, fileId, err := service.ExtractCreateFileResp(resp)
	if err != nil {
		return "", err
	}
	resp, err = Request(service.GetClient(), "PUT", url, &b1)

	if err != nil {
		return "", err
	}

	err = service.WaitForAvailable(id)
	if err != nil {
		return "", err
	}
	return fileId, nil
}

func MultipartUpload(service Service, payload map[string]interface{}, file *os.File, options ...UploadOptions) (string, error) {
	opts, err := defaultUploadOptions(file, options...)
	if err != nil {
		return "", err
	}
	url, err := service.GetUrl("createFile", nil)
	if err != nil {
		return "", err
	}
	resp, err := CreateFile(service, payload, url)
	if err != nil {
		return "", err
	}
	id, url, fileId, err := service.ExtractCreateFileResp(resp)

	if err != nil {
		return "", err
	}
	var parts int = int(opts.ContentLength/opts.ChunkSize) + 1
	if parts > 1000 {
		return "", fmt.Errorf("Number of parts is greater than 1000 update chunk size")
	}
	fmt.Printf("Parts %d\n", parts)

	c := make(chan NonBlocking)
	maxBuff := make(chan int, opts.MaxRoutines)
	nb := []AssembleTag{}
	var wg sync.WaitGroup

	go func() {
		handleUpload(service, c, &nb)
	}()

	for i := 1; i <= parts; i++ {
		maxBuff <- 1
		wg.Add(1)
		go upload(service.GetClient(), i, c, maxBuff, file, &wg, opts, strings.Replace(url, "*", strconv.Itoa(i), -1))

	}

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

func upload(client *http.Client, partNumber int, c chan NonBlocking, d chan int, file *os.File, wg *sync.WaitGroup, options UploadOptions, url string) {
	defer func() { <-d }()
	var startIndex int64 = int64((partNumber - 1)) * options.ChunkSize
	var endIndex int64 = Min(startIndex+options.ChunkSize, options.ContentLength)
	b1 := make([]byte, endIndex-startIndex)

	_, err := file.ReadAt(b1, startIndex)
	if err != nil {
		log.Panic(err)
	}
	resp, err := Request(client, "PUT", url, &b1)
	if err != nil {
		log.Panic(err)
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("Request: %d\n", partNumber)
	}

	c <- NonBlocking{
		Response:   resp,
		Error:      err,
		PartNumber: partNumber,
	}
	wg.Done()
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
