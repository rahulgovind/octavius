package main

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

// The state of our group consists of exactly two
// variables.
// 1) The list of committed files
// 2) The members in the membership list
// We try to maintain the following invariant at all times during execution. For example,
// - The given process must be a valid replica of every file in the state
type State struct {
	lock  sync.RWMutex
	ml    *MemberList
	fr    *FileRing
	owner Member
}

func NewState(owner Member) *State {
	return &State{
		owner: owner,
		fr:    NewFileRing(),
		ml:    &MemberList{},
	}
}

// The state can be updated through the following actions
// 1) AddMember
// 2) RemoveMember
// 3) AddFile

// Returns a 2-tuple (should Commit, already Committed) where
// The first element says whether or not we have the right replica for the file
// The second element says whether the file existed previously in the ring
func (s *State) CommitFile(f File) (bool, bool, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Is this the right replica for the file?
	if s.fr.InRange(f.Id()) {

		primaryRange := s.ml.GetPrimaryRange(s.owner)
		// This is the right replica for the file.
		// But does it already have the file?
		exists := s.fr.Contains(f)
		if !exists {
			// Nope. File isn't already in the file ring. Add the file.
			s.fr.AddFile(f)
			log.Debugf("COMMIT: Committed file %v", f.File)
			log.Debugf("File type: %v", f.Type)
		}
		return true, exists, primaryRange.InRange(f.Id())
	}
	return false, false, false
}

// AddMember adds a member to the state and returns the list of "affected" files and
// whether or not the member was actually added.
// Some of the files might be deleted
func (s *State) addMember(member Member) ([]File, bool, Range) {
	s.lock.Lock()
	defer s.lock.Unlock()

	added := s.ml.Add(member)

	var affected []File

	if added {
		start, end, err := s.ml.GetRange(s.owner)
		if err != nil {
			log.Fatal("Owner not in member list")
		}
		affected = s.fr.UpdateRange(start, end)
		start, end, err = s.ml.GetRange(member)
		if err != nil {
			log.Fatal("Unexpected error. Member just inserted not in list.")
		}
		affected = append(affected, s.fr.Search(start, end)...)
	}

	return affected, added, s.ml.GetPrimaryRange(s.owner)
}

// Returns affected files
func (s *State) removeMember(member Member) ([]File, bool, Range) {
	s.lock.Lock()
	defer s.lock.Unlock()

	exists := s.ml.Contains(member)
	if !exists {
		return []File{}, false, Range{empty: true}
	}

	oldPrimary := s.ml.GetPrimaryRange(s.owner)
	start, end, err := s.ml.GetRange(member)
	affected := s.fr.Search(start, end)
	s.ml.Remove(member)
	newPrimary := s.ml.GetPrimaryRange(s.owner)

	start, end, err = s.ml.GetRange(s.owner)
	if err != nil {
		log.Fatal("Owner not in member list")
	}
	affected = append(affected, s.fr.UpdateRange(start, end)...)

	s.LogPrintMembers()

	log.Debug("Old Primary", oldPrimary)
	log.Debug("New Primary", newPrimary)

	return affected, true, MemberRangeDifference(newPrimary, oldPrimary)
}

func (s *State) setMembers(members []Member) ([]File, Range) {
	s.lock.Lock()
	defer s.lock.Unlock()

	log.Debugf("Setting to new memberlist")
	s.ml.Set(members)
	s.LogPrintMembers()
	var result []File = nil

	start, end, err := s.ml.GetRange(s.owner)
	primaryRange := s.ml.GetPrimaryRange(s.owner)
	if err != nil {
		log.Fatal("Owner not in member list", err)
	}

	result = s.fr.Search(0, M-1)
	s.fr.UpdateRange(start, end)

	s.LogPrintMembers()
	return result, primaryRange
}

func (s *State) Successors(n int) []Member {
	return s.ml.Successors(s.owner, n)
}

func (s *State) RandomMembers(b int) []Member {
	return s.ml.RandomMembers(s.owner, b)
}

func (s *State) NumMembers() int {
	return s.ml.Length()
}

func (s *State) FindReplicas(sdfsFileName string) []Member {
	return s.ml.FindReplicas(sdfsFileName)
}

func (s *State) PrintStore() {
	s.fr.PrintStore()
}

func (s *State) PrintMembers() {
	s.ml.PrintMembers()
}

func (s *State) LogPrintMembers() {
	s.ml.LogPrintMembers()
}
