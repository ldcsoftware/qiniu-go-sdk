package kodo

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodocli"
	"github.com/qiniupd/qiniu-go-sdk/x/url.v7"
)

type PutPolicy kodocli.PutPolicy

// ----------------------------------------------------------

// 根据空间(Bucket)的域名，以及文件的 key，获得 baseUrl。
// 如果空间是 public 的，那么通过 baseUrl 可以直接下载文件内容。
// 如果空间是 private 的，那么需要对 baseUrl 进行私有签名得到一个临时有效的 privateUrl 进行下载。
//
func MakeBaseUrl(domain, key string) (baseUrl string) {

	return "http://" + domain + "/" + url.Escape(key)
}

// ----------------------------------------------------------

type GetPolicy struct {
	Expires uint32
}

func (p *Client) MakePrivateUrl(baseUrl string, policy *GetPolicy) (privateUrl string) {

	var expires int64
	if policy == nil || policy.Expires == 0 {
		expires = 3600
	} else {
		expires = int64(policy.Expires)
	}
	deadline := time.Now().Unix() + expires

	if strings.Contains(baseUrl, "?") {
		baseUrl += "&e="
	} else {
		baseUrl += "?e="
	}
	baseUrl += strconv.FormatInt(deadline, 10)

	token := qbox.Sign(p.mac, []byte(baseUrl))
	return baseUrl + "&token=" + token
}

// --------------------------------------------------------------------------------

func (p *Client) MakeUptoken(policy *PutPolicy) string {

	var rr = *policy
	if rr.Expires == 0 {
		rr.Expires = 3600
	}
	rr.Expires += uint32(time.Now().Unix())
	b, _ := json.Marshal(&rr)
	return qbox.SignWithData(p.mac, b)
}

// ----------------------------------------------------------
