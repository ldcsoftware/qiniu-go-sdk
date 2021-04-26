package operation

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/auth/qbox"
)

type Downloader struct {
	bucket      string
	ioHosts     []string
	ioSelector  *HostSelector
	credentials *qbox.Mac
	queryer     *Queryer
	retry       int

	hostPin        *HostPin
	downloadClient *http.Client
}

func NewDownloader(c *Config) *Downloader {
	mac := qbox.NewMac(c.Ak, c.Sk)
	var queryer *Queryer = nil
	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	downloadClient := &http.Client{
		Transport: NewTransport(c.DialTimeoutMs),
		Timeout:   10 * time.Minute,
	}

	d := &Downloader{
		bucket:      c.Bucket,
		ioHosts:     dupStrings(c.IoHosts),
		credentials: mac,
		queryer:     queryer,
		retry:       c.Retry,

		hostPin:        NewHostPin(c.HostPinTimeMs),
		downloadClient: downloadClient,
	}
	update := func() []string {
		if d.queryer != nil {
			return d.queryer.QueryIoHosts(false)
		}
		return nil
	}
	d.ioSelector = NewHostSelector(d.ioHosts, update, 0, c.PunishTimeS, shouldRetry)
	return d
}

func NewDownloaderV2() *Downloader {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewDownloader(c)
}

func (d *Downloader) Retry(f func(host string) error) (err error) {
	for i := 0; i < d.retry; i++ {
		host := d.hostPin.Unpin()
		if host == "" {
			host = d.ioSelector.SelectHost()
		}
		err = f(host)
		if shouldRetry(err) {
			d.ioSelector.SetPunish(host)
			elog.Info("download try failed. punish host", host, i, err)
			continue
		}
		d.hostPin.Pin(host)
		break
	}
	return err
}

func (d *Downloader) DownloadFile(key, path string) (f *os.File, err error) {
	d.Retry(func(host string) error {
		f, err = d.downloadFileInner(key, host, path)
		return err
	})
	return
}

func (d *Downloader) DownloadBytes(key string) (data []byte, err error) {
	d.Retry(func(host string) error {
		data, err = d.downloadBytesInner(key, host)
		return err
	})
	return
}

func (d *Downloader) DownloadRangeBytes(key string, offset, size int64, initBuf []byte) (l int64, data []byte, err error) {
	d.Retry(func(host string) error {
		l, data, err = d.downloadRangeBytesInner(key, host, offset, size, initBuf)
		return err
	})
	return
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func (d *Downloader) downloadFileInner(key, host, path string) (*os.File, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	length, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	fmt.Println("remote path", key)
	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "")
	if length != 0 {
		r := fmt.Sprintf("bytes=%d-", length)
		req.Header.Set("Range", r)
		fmt.Println("continue download")
	}

	response, err := d.downloadClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return f, nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		return nil, errors.New(response.Status)
	}
	ctLength := response.ContentLength
	n, err := io.Copy(f, response.Body)
	if err != nil {
		return nil, err
	}
	if ctLength != n {
		elog.Warn("download length not equal", ctLength, n)
	}
	f.Seek(0, io.SeekStart)
	return f, nil
}

func (d *Downloader) downloadBytesInner(key, host string) ([]byte, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	response, err := d.downloadClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, errors.New(response.Status)
	}
	return ioutil.ReadAll(response.Body)
}

func generateRange(offset, size int64) string {
	if offset == -1 {
		return fmt.Sprintf("bytes=-%d", size)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
}

func readAll(r io.Reader, initBuf []byte) (b []byte, err error) {
	// If the buffer overflows, we will get bytes.ErrTooLarge.
	// Return that as an error. Any other panic remains.
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		if panicErr, ok := e.(error); ok && panicErr == bytes.ErrTooLarge {
			err = panicErr
		} else {
			panic(e)
		}
	}()
	buf := bytes.NewBuffer(initBuf[:0])
	_, err = buf.ReadFrom(r)
	return buf.Bytes(), err
}

func (d *Downloader) downloadRangeBytesInner(key, host string, offset, size int64, initBuf []byte) (int64, []byte, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return -1, nil, err
	}

	req.Header.Set("Range", generateRange(offset, size))
	response, err := d.downloadClient.Do(req)
	if err != nil {
		return -1, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusPartialContent {
		return -1, nil, errors.New(response.Status)
	}

	rangeResponse := response.Header.Get("Content-Range")
	if rangeResponse == "" {
		return -1, nil, errors.New("no content range")
	}

	l, err := getTotalLength(rangeResponse)
	if err != nil {
		return -1, nil, err
	}
	b, err := readAll(response.Body, initBuf)
	return l, b, err
}

func getTotalLength(crange string) (int64, error) {
	cr := strings.Split(crange, "/")
	if len(cr) != 2 {
		return -1, errors.New("wrong range " + crange)
	}

	return strconv.ParseInt(cr[1], 10, 64)
}
