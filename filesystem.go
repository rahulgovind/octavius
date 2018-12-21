package octavius

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
)

type FileSystem struct {
	root      string
	baseAddr  string
	addr      string
	countLock sync.Mutex
	fileCount map[string]int
}

var dfs *FileSystem

func InitFileSystem(root string, addr string) {
	dfs = &FileSystem{root: root, baseAddr: addr, fileCount: make(map[string]int)}
	CreateFileDir(root)
	go dfs.StartServer()
}

type Handler struct{ root string }

func (h Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	f, err := os.Open(filepath.Join(h.root, req.RequestURI))
	if err != nil {
		io.WriteString(w, "Error 500")
		return
	}
	defer f.Close()

	_, err = io.Copy(w, f)
	if err != nil {
		io.WriteString(w, "Error 500")
		return
	}
}

func (fs *FileSystem) GetHTTPAddr() string {
	return fs.addr
}

func GetHTTPAddr() string {
	return dfs.GetHTTPAddr()
}

func (fs *FileSystem) StartServer() {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Unable to set up HTTP Listener: %v", err)
	}

	port := listener.Addr().(*net.TCPAddr).Port
	log.Debugf("Starting HTTP File Server on Port %v", port)
	fs.addr = fmt.Sprintf("%v:%v", fs.baseAddr, port)

	err = http.Serve(listener, Handler{root: fs.root})
	if err != nil {
		log.Fatal(err)
	}
}

func (fs *FileSystem) Open(filename string) (*os.File, error) {
	return os.Open(path.Join(fs.root, filename))
}

func Open(filename string) (*os.File, error) {
	return dfs.Open(filename)
}

func (fs *FileSystem) Create(filename string) (*os.File, error) {
	f, err := os.Create(fs.abs(filename))
	if err != nil {
		return nil, err
	}

	if filename != "NULL" {
		fs.countLock.Lock()
		defer fs.countLock.Unlock()
		fs.fileCount[fs.abs(filename)] = 1
	}
	return f, err
}

func Create(filename string) (*os.File, error) {
	return dfs.Create(filename)
}

func (fs *FileSystem) abs(filename string) string {
	return path.Join(fs.root, filename)
}
func (fs *FileSystem) CopyFile(srcFile, dstFile string) error {
	return copyFile(path.Join(fs.root, srcFile), path.Join(fs.root, dstFile))
}

func CopyFile(srcFile, dstFile string) error {
	return dfs.CopyFile(srcFile, dstFile)
}

func (fs *FileSystem) CreateCopy(filename string) {
	if filename != "NULL" {
		fs.countLock.Lock()
		defer fs.countLock.Unlock()
		if _, ok := fs.fileCount[fs.abs(filename)]; ok {
			fs.fileCount[fs.abs(filename)] += 1
		}
	}
}

func CreateCopy(filename string) {
	dfs.CreateCopy(filename)
}

func (fs *FileSystem) FileSize(filename string) int {
	fi, err := os.Stat(path.Join(fs.root, filename))
	if err != nil {
		log.Fatal(err)
		return -1
	}
	return int(fi.Size())
}

func FileSize(filename string) int {
	return dfs.FileSize(filename)
}

func (fs *FileSystem) Delete(filename string) error {
	fs.countLock.Lock()
	defer fs.countLock.Unlock()
	if filename == "NULL" {
		return nil
	}
	abs := fs.abs(filename)
	if _, ok := fs.fileCount[abs]; ok {
		fs.fileCount[abs] -= 1
		if fs.fileCount[abs] == 0 {
			err := os.Remove(abs)
			delete(fs.fileCount, abs)
			return err
		}
		return nil
	} else {
		return errors.New("file does not exist")
	}
	return nil
}


func Delete(filename string) error {
	return dfs.Delete(filename)
}
