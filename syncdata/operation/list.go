package operation

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodo"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano() | int64(os.Getpid())))

func randomNext() uint32 {
	return random.Uint32()
}

type Lister struct {
	bucket      string
	rsHosts     []string
	rsSelector  *HostSelector
	rsfSelector *HostSelector
	upHosts     []string
	rsfHosts    []string
	credentials *qbox.Mac
	queryer     *Queryer
	retry       int
	transport   http.RoundTripper
	hostPin     *HostPin
}

type FileStat struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (l *Lister) RetryRs(f func(host string) error) (err error) {
	for i := 0; i < l.retry; i++ {
		host := l.hostPin.Unpin()
		if host == "" {
			host = l.rsSelector.SelectHost()
		}
		err = f(host)
		if shouldRetry(err) {
			l.rsSelector.SetPunish(host)
			elog.Info("rs try failed. punish host", host, err, i)
			continue
		}
		l.hostPin.Pin(host)
		break
	}
	return err
}

func (l *Lister) RetryRsf(f func(host string) error) (err error) {
	for i := 0; i < l.retry; i++ {
		host := l.rsfSelector.SelectHost()
		err = f(host)
		if shouldRetry(err) {
			l.rsfSelector.SetPunish(host)
			elog.Info("rsf try failed. punish host", host, err, i)
			continue
		}
		break
	}
	return err
}

func (l *Lister) Delete(ctx context.Context, key string) (err error) {
	l.RetryRs(func(host string) error {
		bucket := l.newBucket(host, "")
		err = bucket.Delete(ctx, key)
		return err
	})
	return
}

func (l *Lister) BatchDelete(ctx context.Context, key ...string) (rets []kodo.BatchItemRet, err error) {
	l.RetryRs(func(host string) error {
		bucket := l.newBucket(host, "")
		rets, err = bucket.BatchDelete(ctx, key...)
		return err
	})
	return
}

func (l *Lister) Stat(ctx context.Context, key string) (entry kodo.Entry, err error) {
	l.RetryRs(func(host string) error {
		bucket := l.newBucket(host, "")
		entry, err = bucket.Stat(ctx, key)
		return err
	})
	return
}

func (l *Lister) ListPrefix(ctx context.Context, prefix, marker string, limit int) (entrys []kodo.ListItem, markerOut string, err error) {
	l.RetryRsf(func(host string) error {
		bucket := l.newBucket("", host)
		entrys, markerOut, err = bucket.List(ctx, prefix, marker, limit)
		if err == io.EOF {
			return nil
		}
		return err
	})
	return
}

func NewLister(c *Config) *Lister {
	mac := qbox.NewMac(c.Ak, c.Sk)
	var queryer *Queryer = nil
	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	l := &Lister{
		bucket:      c.Bucket,
		rsHosts:     dupStrings(c.RsHosts),
		upHosts:     dupStrings(c.UpHosts),
		rsfHosts:    dupStrings(c.RsfHosts),
		credentials: mac,
		queryer:     queryer,
		retry:       c.Retry,
		transport:   NewTransport(c.DialTimeoutMs),
		hostPin:     NewHostPin(c.HostPinTimeMs),
	}
	updateRs := func() []string {
		if l.queryer != nil {
			return l.queryer.QueryRsHosts(false)
		}
		return nil
	}
	l.rsSelector = NewHostSelector(l.rsHosts, updateRs, 0, c.PunishTimeS, shouldRetry)
	updateRsf := func() []string {
		if l.queryer != nil {
			return l.queryer.QueryRsfHosts(false)
		}
		return nil
	}
	l.rsfSelector = NewHostSelector(l.rsfHosts, updateRsf, 0, c.PunishTimeS, shouldRetry)
	return l
}

func NewListerV2() *Lister {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewLister(c)
}

func (l *Lister) newBucket(host, rsfHost string) kodo.Bucket {
	cfg := kodo.Config{
		AccessKey: l.credentials.AccessKey,
		SecretKey: string(l.credentials.SecretKey),
		RSHost:    host,
		RSFHost:   rsfHost,
		Transport: l.transport,
	}
	client := kodo.New(0, &cfg)
	return client.Bucket(l.bucket)
}
