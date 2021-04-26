package operation

import (
	"context"
	"fmt"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/x/rpc.v7"
)

type UserCenter struct {
	bucket      string
	ucHosts     []string
	ucSelector  *HostSelector
	credentials *qbox.Mac
	queryer     *Queryer
	retry       int

	rpc.Client
}

type BucketQuota struct {
	// 空间存储量配额信息
	Size int64 `json:"size"`

	// 空间文件数配置信息
	Count int64 `json:"count"`
}

type FileStorageRet struct {
	FileNum     int64 `json:"file_num"`
	StorageSize int64 `json:"storage_size"`
}

func NewUserCenter(c *Config) *UserCenter {
	mac := qbox.NewMac(c.Ak, c.Sk)
	var queryer *Queryer = nil
	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	Client := rpc.Client{qbox.NewClient(mac, NewTransport(c.DialTimeoutMs))}

	u := &UserCenter{
		bucket:      c.Bucket,
		ucHosts:     dupStrings(c.UcHosts),
		credentials: mac,
		queryer:     queryer,
		retry:       c.Retry,
		Client:      Client,
	}
	update := func() []string {
		if u.queryer != nil {
			return u.queryer.QueryUcHosts(false)
		}
		return nil
	}
	u.ucSelector = NewHostSelector(u.ucHosts, update, 0, c.PunishTimeS, shouldRetry)
	return u
}

func NewUserCenterV2() *UserCenter {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewUserCenter(c)
}

func (u *UserCenter) Retry(f func(host string) error) (err error) {
	for i := 0; i < u.retry; i++ {
		host := u.ucSelector.SelectHost()
		err = f(host)
		if shouldRetry(err) {
			u.ucSelector.SetPunish(host)
			elog.Info("uc try failed. punish host", host, i, err)
			continue
		}
		break
	}
	return err
}

func (d *UserCenter) GetBucketQuota(ctx context.Context) (stats BucketQuota, err error) {
	d.Retry(func(host string) error {
		stats, err = d.getBucketQuota(ctx, host)
		return err
	})
	return
}

func (u *UserCenter) getBucketQuota(ctx context.Context, host string) (stats BucketQuota, err error) {
	url := fmt.Sprintf("%s/getbucketquota/%s", host, u.bucket)
	err = u.Client.Call(ctx, &stats, "POST", url)
	return
}

func (u *UserCenter) GetBucketUsage(ctx context.Context) (stats FileStorageRet, err error) {
	u.Retry(func(host string) error {
		stats, err = u.getBucketUsage(ctx, host)
		return err
	})
	return
}

func (u *UserCenter) getBucketUsage(ctx context.Context, host string) (stats FileStorageRet, err error) {
	params := map[string][]string{
		"bucket": {u.bucket},
	}
	url := fmt.Sprintf("%s/v2/bucketInfo?fs=true", host)
	err = u.Client.CallWithForm(ctx, &stats, "POST", url, params)
	return
}
