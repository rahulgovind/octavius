package octavius

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

type AtomicCounter struct {
	count int
	lock  sync.Mutex
}

// Increment adds 1 to the count and returns the old value
func (ac *AtomicCounter) Increment() int {
	ac.lock.Lock()
	defer ac.lock.Unlock()
	oldCount := ac.count
	ac.count += 1
	return oldCount
}

type Semaphore struct {
	s chan bool
}

func NewSemaphore(n int) *Semaphore {
	r := &Semaphore{make(chan bool, n)}
	// Fill semaphore
	for i := 0; i < n; i++ {
		r.s <- true
	}
	return r
}

func (sem *Semaphore) Acquire() {
	<-sem.s
}

func (sem *Semaphore) Release() {
	sem.s <- true
}

var globalCounter AtomicCounter

func Next() int {
	return globalCounter.Increment()
}

// https://stackoverflow.com/questions/33450980/golang-remove-all-contents-of-a-directory
func CreateFileDir(dir string) {

	dir_exists := true
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.Mkdir(dir, os.ModePerm)
		if err != nil {
			log.Debug("Fail to create File directory", err)
		}
		log.Debug("mkdir directory!")
		dir_exists = false
	}
	log.Debug("dir_exists:", dir_exists)
	if dir_exists {
		subDir, _ := ioutil.ReadDir(dir)
		for _, d := range subDir {
			os.RemoveAll(path.Join([]string{dir, d.Name()}...))
		}
	}
}

func ByteCountDecimal(b int) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}

/* Stack overflow - https://stackoverflow.com/questions/21060945/simple-way-to-copy-a-file-in-golang */

// CopyFile copies a File from src to dst. If src and dst files exist, and are
// the same, then return success. Otherwise, attempt to create a hard Link
// between the two files. If that fail, copy the File contents from src to dst.
func copyFile(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source File %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination File %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}
	//err = copyFileContents(src, dst)
	return
}

// copyFileContents copies the contents of the File named src to the File named
// by dst. The File will be created if it does not already exist. If the
// destination File exists, all it's contents will be replaced by the contents
// of the source File.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

/* End stackoverflow */
