package shared

import "net/http"

type Service interface {
	GetClient() *http.Client
	GetUrl(action string, replaceMap map[string]string) (string, error)
	ExtractCreateFileResp(resp *http.Response) (string, string, string, error)
	WaitForAvailable(id string) error
	Assemble(id string, parts []AssembleTag) error
	CreateTag(etag string, partNumber int) AssembleTag
}
