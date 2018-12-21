package main

import (
	"fmt"
	"github.com/emirpasic/gods/sets/treeset"
	"github.com/emirpasic/gods/utils"
	"github.com/rahulgovind/octavius"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Each File links to a local File
type File struct {
	File          string
	Sequence      int64
	Link          string
	Tombstone     bool
	AutoTombstone bool
	Type          int
	Updated       time.Time
	counter       AtomicCounter
}

const (
	BinaryFile  = 1
	ProgramFile = 2
	JsonFile    = 3
)

func (f *File) LogPrint() {
	log.Debugf("Name:%v\tSequence:%v\tAutotombsotne:%v\tType:%v\n", f.File, f.Sequence, f.AutoTombstone, f.Type)
}

func (f *File) IsProgram() bool {
	return f.Type == ProgramFile
}

func (f *File) IsJson() bool {
	return f.Type == JsonFile
}

func (f *File) Deleted() bool {
	return f.Tombstone || f.AutoTombstone
}

func (f *File) Copy() File {
	octavius.CreateCopy(f.Link)
	return *f
}

func (f *File) Delete() {
	err := octavius.Delete(f.Link)
	if err != nil {
		debug.PrintStack()
		log.Fatal("Cannot delete file: ", err)
	}
}

type EncodedFilename string

type FileMap struct {
	versionMap map[int64]File
	versions   *treeset.Set
}

func NewFileMap() *FileMap {
	return &FileMap{
		versionMap: make(map[int64]File),
		versions:   treeset.NewWith(utils.Int64Comparator),
	}
}

func (fm *FileMap) InsertFile(f File) {
	fm.versionMap[f.Sequence] = f
	fm.versions.Add(f.Sequence)
}

func (fm *FileMap) Files() []File {
	var result []File
	for _, v := range fm.versionMap {
		result = append(result, v)
	}
	return result
}

func (fm *FileMap) SortedFiles() []File {
	var result []File
	it := fm.versions.Iterator()

	for it.Next() {
		v := it.Value().(int64)
		result = append(result, fm.versionMap[v])
	}
	return result
}

type FileRing struct {
	hashMap map[int64]FileMap
	start   int64
	end     int64
	lock    sync.RWMutex
}

func NewFileRing() *FileRing {
	return &FileRing{
		hashMap: make(map[int64]FileMap),
		start:   0,
		end:     M - 1,
	}
}

func (f *File) Id() int64 {
	return hashInt(f.File)
}

func (f *File) toFilename(n int) EncodedFilename {
	return EncodedFilename(fmt.Sprintf("%v_%v_%v_%v", f.File, f.Sequence, f.Tombstone, n))
}

func (f *File) Equal(f2 *File) bool {
	return f.File == f2.File && f.Sequence == f2.Sequence
}

func (ef *EncodedFilename) toFile(dir string) File {
	l := strings.Split(string(*ef), "_")

	filename := l[0]
	sequence, _ := strconv.ParseInt(l[1], 10, 64)
	tombstone, _ := strconv.ParseBool(l[2])
	return File{
		File:      filename,
		Sequence:  sequence,
		Link:      path.Join(dir, filename),
		Tombstone: tombstone,
	}
}

// InRange checks if an ID falls within the range of the file ring
func (fr *FileRing) InRange(x int64) bool {
	fr.lock.RLock()
	defer fr.lock.RUnlock()
	return InRange(x, fr.start, fr.end, M)
}

// UpdateRange updates the range of the file ring. It uncommits files not in this range and returns these files
func (fr *FileRing) UpdateRange(start, end int64) []File {
	fr.lock.Lock()
	defer fr.lock.Unlock()

	var deleted []File

	for k, v := range fr.hashMap {
		if !InRange(k, start, end, M) {
			deleted = append(deleted, v.SortedFiles()...)
			delete(fr.hashMap, k)
		}
	}
	fr.start = start
	fr.end = end

	return deleted
}

// Contains checks if the file ring contains the given file
func (fr *FileRing) Contains(f File) bool {
	fr.lock.RLock()
	defer fr.lock.RUnlock()

	h := f.Id()
	if m, ok := fr.hashMap[h]; ok {
		found, ok := m.versionMap[f.Sequence]
		if ok {
			// We have assumed till now that there are no hash cnoflicts. What if there is one?
			// Panic for now. Fix later :P
			if found.File != f.File {
				log.Fatal("Hash conflict!")
			}
			if found.AutoTombstone == false && f.AutoTombstone == true {
				return false
			}
			return true
		}
	}
	return false
}

// Add File commits a file to the file ring
// Returns whether or not the file was actually committed
// File might not be connected if we already have the file, or if there is a tombstone with a higher
// sequence number
func (fr *FileRing) AddFile(f File) bool {
	fr.lock.Lock()
	defer fr.lock.Unlock()
	h := f.Id()

	if m, ok := fr.hashMap[h]; ok {
		it := m.versions.Iterator()
		if it.First() {
			firstVersion := it.Value().(int64)
			if firstVersion >= f.Sequence && m.versionMap[firstVersion].Tombstone {
				f.Delete()
				return false
			}
		}

		// If file is a tombstone, uncommit all the files with lower sequence number (older versions)
		if f.Tombstone {
			// Can't delete from treeset while iterating over it. Hence, create a copy first of the versions
			// that need to be deleted
			var oldVersions []int64
			it = m.versions.Iterator()
			for it.Next() {
				oldVersions = append(oldVersions, it.Value().(int64))
			}

			for _, v := range oldVersions {
				oldFile := m.versionMap[v]
				delete(m.versionMap, v)
				m.versions.Remove(v)
				oldFile.Delete()
			}
		}

		m.InsertFile(f)
	} else {
		// No version of file exists yet. Create the store for this file
		fm := NewFileMap()
		fm.InsertFile(f)
		fr.hashMap[h] = *fm
	}
	return true
}

// Search returns all the files with IDs that fall within the given range [start, end] (Note: Both inclusive)
// Super slow. O(Number of file names)
func (fr *FileRing) Search(start, end int64) []File {
	fr.lock.RLock()
	defer fr.lock.RUnlock()

	r := make([]File, 0)
	for k, v := range fr.hashMap {
		if InRange(k, start, end, M) {
			r = append(r, v.SortedFiles()...)
		}
	}
	return CopyAll(r)
}

func (fr *FileRing) Find(sdfsFileName string) []File {
	x := hashInt(sdfsFileName)
	return fr.Search(x, x)
}

// PrintStore prints all the committed files. Nicely numbers them too.
func (fr *FileRing) PrintStore() {
	fr.lock.RLock()
	defer fr.lock.RUnlock()
	fmt.Println("Files being stored at this machine:")
	for _, m := range fr.hashMap {
		j := 0
		for _, f := range m.SortedFiles() {
			if f.Tombstone {
				fmt.Printf("%v TOMBSTONE\n", f.File)
			} else {
				j += 1
				if f.AutoTombstone {
					fmt.Printf("%v (Version %v)\t%v\tDELETED\n", f.File, j, f.Sequence)
				} else {
					fmt.Printf("%v (Version %v)\t%v\n", f.File, j, f.Sequence)
				}
			}
		}
	}
}

func CreateTombstone(sdfsFilename string) File {
	return File{
		File:      sdfsFilename,
		Sequence:  Timestamp(),
		Link:      "NULL",
		Tombstone: true,
		Type:      BinaryFile,
		Updated:   time.Now(),
	}
}

func CreateAutoTombstone(f File) File {
	var f2 = f.Copy()
	f2.AutoTombstone = true
	f2.Link = "NULL"
	f2.Updated = time.Now()

	return f2
}

func CreateLocalFile(absPath string, sdfsFilename string, filetype int) File {
	baseFilename := fmt.Sprintf("%v-%v", path.Base(absPath), octavius.Next())
	f, err := octavius.Create(baseFilename)
	if err != nil {
		log.Fatal("Unable to open file: ", err)
	}
	srcFile, err := os.Open(absPath)
	defer srcFile.Close()

	if err != nil {
		log.Fatal("Unable to open source file: ", err)
	}
	io.Copy(f, srcFile)
	// TODO: Make link instead of copy

	return File{
		File:      sdfsFilename,
		Sequence:  Timestamp(),
		Link:      baseFilename,
		Tombstone: false,
		Type:      filetype,
		Updated:   time.Now(),
	}
}

func CreateTempFile(r io.Reader, sdfsFilename string, sequence int64, fileType int) File {
	if sequence == -1 {
		sequence = Timestamp()
	}

	baseFilename := fmt.Sprintf("%v-%v", sequence, octavius.Next())
	f, err := octavius.Create(baseFilename)
	if err != nil {
		log.Fatal("Unable to create temporary file")
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	if err != nil {
		log.Fatal("Unable to write to temporary file")
	}

	return File{
		File:      sdfsFilename,
		Sequence:  sequence,
		Link:      baseFilename,
		Tombstone: false,
		Type:      fileType,
		Updated:   time.Now(),
	}
}

func CopyAll(files []File) []File {
	result := make([]File, 0)
	for _, file := range files {
		result = append(result, file.Copy())
	}
	return result
}

func DeleteAll(files []File) {
	for _, file := range files {
		file.Delete()
	}
}
