package operation

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/x/bytes.v7"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodo"
	q "github.com/qiniupd/qiniu-go-sdk/api.v8/kodocli"
)

type Uploader struct {
	bucket        string
	upHosts       []string
	upSelector    *HostSelector
	credentials   *qbox.Mac
	partSize      int64
	upConcurrency int
	queryer       *Queryer
	retry         int
	transport     http.RoundTripper
}

func (p *Uploader) makeUptoken(policy *kodo.PutPolicy) string {
	var rr = *policy
	if rr.Expires == 0 {
		rr.Expires = 3600 + uint32(time.Now().Unix())
	}
	b, _ := json.Marshal(&rr)
	return qbox.SignWithData(p.credentials, b)
}

func (p *Uploader) Retry(uploader *q.Uploader, f func() error) (err error) {
	for i := 0; i < p.retry; i++ {
		err = f()
		if shouldRetry(err) {
			elog.Info("upload try failed. punish host", i, err)
			continue
		}
		break
	}
	return err
}

func (p *Uploader) UploadData(ctx context.Context, key string, data []byte, ret interface{}) (err error) {
	t := time.Now()
	defer func() {
		elog.Info("up time ", key, time.Now().Sub(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}

	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.transport,
		HostSelector:   p.upSelector,
	})
	return p.Retry(&uploader, func() error {
		return uploader.Put2(ctx, ret, upToken, key, bytes.NewReader(data), int64(len(data)), nil)
	})
}

func (p *Uploader) UploadDataReader(ctx context.Context, data io.ReadSeeker, size int, key string) (err error) {
	t := time.Now()
	defer func() {
		elog.Info("up time ", key, time.Now().Sub(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}

	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.transport,
		HostSelector:   p.upSelector,
	})

	return p.Retry(&uploader, func() error {
		err := uploader.Put2(ctx, nil, upToken, key, ioutil.NopCloser(data), int64(size), nil)
		if err == nil {
			return nil
		}
		_, err = data.Seek(0, io.SeekStart)
		return err
	})
}

func (p *Uploader) UploadDataReaderAt(ctx context.Context, key string, data io.ReaderAt, size int64, ret interface{}) (err error) {
	t := time.Now()
	defer func() {
		elog.Info("up time ", key, time.Now().Sub(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}

	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.transport,
		HostSelector:   p.upSelector,
	})

	return p.Retry(&uploader, func() error {
		var r io.Reader = io.NewSectionReader(data, 0, size)
		return uploader.Put2(ctx, ret, upToken, key, ioutil.NopCloser(r), int64(size), nil)
	})
}

func (p *Uploader) Upload(ctx context.Context, file string, key string) (err error) {
	t := time.Now()
	defer func() {
		elog.Info("up time ", key, time.Now().Sub(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}
	upToken := p.makeUptoken(&policy)

	f, err := os.Open(file)
	if err != nil {
		elog.Info("open file failed: ", file, err)
		return err
	}
	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		elog.Info("get file stat failed: ", err)
		return err
	}

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.transport,
		HostSelector:   p.upSelector,
	})

	if fInfo.Size() <= p.partSize {
		return p.Retry(&uploader, func() error {
			err := uploader.Put2(ctx, nil, upToken, key, ioutil.NopCloser(f), fInfo.Size(), nil)
			if err == nil {
				return nil
			}
			_, err = f.Seek(0, io.SeekStart)
			return err
		})
	}

	return p.Retry(&uploader, func() error {
		return uploader.Upload(ctx, nil, upToken, key, newReaderAtNopCloser(f), fInfo.Size(), nil,
			func(partIdx int, etag string) {
				elog.Info("callback", partIdx, etag)
			})
	})
}

func (p *Uploader) UploadWithDataChan(ctx context.Context, key string, concurrency int, dataCh chan q.PartData, ret interface{}, initNotify func(suggestedPartSize int64)) (err error) {
	t := time.Now()
	defer func() {
		elog.Info("up time ", key, time.Now().Sub(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}
	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		Concurrency:  concurrency,
		Transport:    p.transport,
		HostSelector: p.upSelector,
	})

	return uploader.UploadWithDataChan(ctx, ret, upToken, key, dataCh, nil, initNotify,
		func(partIdx int, etag string) {
			elog.Info("callback", partIdx, etag)
		})
}

func NewUploader(c *Config) *Uploader {
	mac := qbox.NewMac(c.Ak, c.Sk)
	var queryer *Queryer = nil
	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	p := &Uploader{
		bucket:        c.Bucket,
		upHosts:       dupStrings(c.UpHosts),
		credentials:   mac,
		partSize:      c.PartSize,
		upConcurrency: c.UpConcurrency,
		queryer:       queryer,
		retry:         c.Retry,
		transport:     NewTransport(c.DialTimeoutMs),
	}
	update := func() []string {
		if p.queryer != nil {
			return p.queryer.QueryUpHosts(false)
		}
		return nil
	}
	p.upSelector = NewHostSelector(p.upHosts, update, 0, c.PunishTimeS, shouldRetry)
	return p
}

func NewUploaderV2() *Uploader {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewUploader(c)
}

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

type readerAtNopCloser struct {
	io.ReaderAt
}

func (readerAtNopCloser) Close() error { return nil }

// newReaderAtNopCloser returns a readerAtCloser with a no-op Close method wrapping
// the provided ReaderAt r.
func newReaderAtNopCloser(r io.ReaderAt) readerAtCloser {
	return readerAtNopCloser{r}
}
