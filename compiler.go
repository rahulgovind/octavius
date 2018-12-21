package octavius

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"go/build"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"plugin"
	"strings"
)

type Program struct {
	Source string
	lib    []byte
	p      *plugin.Plugin
}

func (p *Program) Compile() {
	source := p.Source

	base := path.Join(build.Default.GOPATH, "src")
	srcdir, err := ioutil.TempDir(base, "source")
	if err != nil {
		log.Fatal("Unable to create temp package: ", err)
	}
	defer os.RemoveAll(srcdir)
	if !strings.HasPrefix(srcdir, base) {
		log.Fatal("Invalid package. Expected %v/* \tGot %v", base, srcdir)
	}
	pkgName := srcdir[len(base)+1:]

	srcFile, err := os.Create(path.Join(srcdir, "main.go"))
	if err != nil {
		log.Fatal("Unable to create temp file: ", err)
	}
	log.Info("Source", source)
	io.Copy(srcFile, strings.NewReader(source))
	srcFile.Close()

	outfile, err := ioutil.TempFile("", "out")
	if err != nil {
		log.Fatal("Unable to create temp file: ", err)
	}
	defer os.Remove(outfile.Name())
	outfile.Close()

	log.Infof("Building from %v", srcFile.Name())

	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", outfile.Name(), pkgName)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		log.Fatal(err, ": ", stderr.String())
	}

	outfile2, err := os.Open(outfile.Name())
	if err != nil {
		log.Fatal(err, stderr.String())
	}

	var buf bytes.Buffer
	io.Copy(&buf, outfile2)
	p.lib = buf.Bytes()
}

func (p *Program) Load(funcNames ...string) []interface{} {
	sharedlib, err := ioutil.TempFile("", "out")
	if err != nil {
		log.Fatal("Unable to create temp lib: ", err)
	}

	p.Compile()

	io.Copy(sharedlib, bytes.NewReader(p.lib))
	sharedlib.Close()

	// _Do not_ remove file
	defer os.Remove(sharedlib.Name())

	pg, err := plugin.Open(sharedlib.Name())
	if err != nil {
		log.Fatal("Unable to open plugin: ", pg, "\t", err)
	}

	var result []interface{}

	for _, funcName := range funcNames {
		f, err := pg.Lookup(funcName)

		if err != nil {
			log.Fatalf("Unable to load function %v: %v", funcName, err)
		}

		result = append(result, f)
	}
	return result
}
