package octavius

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"time"
)

func Download(addr, dstFile string, w io.Writer) error {
	url := fmt.Sprintf("http://%v/%v", addr, dstFile)
	resp, err := http.Get(url)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	start := time.Now()
	written, err := io.Copy(w, resp.Body)
	elapsed := time.Since(start)

	log.Infof("Response Code: ", resp.Status)
	log.Infof("Writing file took %v", elapsed)
	size := written
	speed := int(float64(written) / elapsed.Seconds())

	log.Infof("Size: %v\tTime to download: %v\tSpeed: %v/s", size, elapsed, ByteCountDecimal(speed))
	if err != nil {
		return err
	}

	return err
}

func DownloadReader(addr, dstFile string) (io.Reader, error) {
	url := fmt.Sprintf("http://%v/%v", addr, dstFile)
	resp, err := http.Get(url)

	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}
