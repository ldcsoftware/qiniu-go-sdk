package kodocli

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

type FileType uint32

const (
	TypeNormal = iota
	TypeLine
	TypeArchive
	FileTypeMax
)

type PutPolicy struct {
	Scope               string   `json:"scope"`
	Expires             uint32   `json:"deadline"`             // 截止时间（以秒为单位）
	InsertOnly          uint16   `json:"insertOnly,omitempty"` // 若非0, 即使Scope为 Bucket:Key 的形式也是insert only
	DetectMime          uint8    `json:"detectMime,omitempty"` // 若非0, 则服务端根据内容自动确定 MimeType
	CallbackFetchKey    uint8    `json:"callbackFetchKey,omitempty"`
	FsizeLimit          int64    `json:"fsizeLimit,omitempty"`
	MimeLimit           string   `json:"mimeLimit,omitempty"`
	SaveKey             string   `json:"saveKey,omitempty"`
	CallbackUrl         string   `json:"callbackUrl,omitempty"`
	CallbackHost        string   `json:"callbackHost,omitempty"`
	CallbackBody        string   `json:"callbackBody,omitempty"`
	CallbackBodyType    string   `json:"callbackBodyType,omitempty"`
	ReturnUrl           string   `json:"returnUrl,omitempty"`
	ReturnBody          string   `json:"returnBody,omitempty"`
	PersistentOps       string   `json:"persistentOps,omitempty"`
	PersistentNotifyUrl string   `json:"persistentNotifyUrl,omitempty"`
	PersistentPipeline  string   `json:"persistentPipeline,omitempty"`
	EndUser             string   `json:"endUser,omitempty"`
	Checksum            string   `json:"checksum,omitempty"` // 格式：<HashName>:<HexHashValue>，目前支持 MD5/SHA1。
	NotifyQueue         string   `json:"notifyQueue,omitempty"`
	NotifyMessage       string   `json:"notifyMessage,omitempty"`
	NotifyMessageType   string   `json:"notifyMessageType,omitempty"`
	DeleteAfterDays     int      `json:"deleteAfterDays,omitempty"`
	FileType            FileType `json:"fileType,omitempty"`
	//内部参数
	OldFh   string `json:"oldFh,omitempty"`
	PutTime int64  `json:"putTime,omitempty"`
	Cond    string `json:"cond,omitempty"` //格式：condKey1=condVal1&condKey2=condVal2,支持hash、mime、fsize、putTime条件，只有条件匹配才会执行覆盖操作
}

func ParseUptoken(uptoken string) (policy PutPolicy, err error) {
	ps := strings.Split(uptoken, ":")
	if len(ps) != 3 {
		err = errors.New("invalid uptoken")
		return
	}

	pb, err := base64.URLEncoding.DecodeString(ps[2])
	if err != nil {
		return
	}
	err = json.Unmarshal(pb, &policy)
	return
}

// ----------------------------------------------------------
