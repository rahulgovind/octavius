package main

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"sort"
	"sync"
)

type Member struct {
	Addr        string
	RPCAddr     string
	Incarnation uint64
}

// Memberlist is maintained as a sorted list at all times
type MemberList struct {
	list []Member
	lock sync.RWMutex
}

type Range struct {
	start int64
	end   int64
	empty bool
}

func (r *Range) InRange(x int64) bool {
	if r.empty {
		return false
	}
	return InRange(x, r.start, r.end, M)
}

/********************************************************************************
*				Member functions
********************************************************************************/
func (m *Member) ProtoMember() *ProtoMember {
	return &ProtoMember{
		Addr:        m.Addr,
		RpcAddr:     m.RPCAddr,
		Incarnation: m.Incarnation,
	}
}

func (m *Member) Id() int64 {
	return hashInt(m.Addr)
}

func ProtoMemberToMember(pm ProtoMember) Member {
	return Member{
		Addr:        pm.Addr,
		RPCAddr:     pm.RpcAddr,
		Incarnation: pm.Incarnation,
	}
}

func ProtoMembersToMembers(members []*ProtoMember) []Member {
	var list []Member
	for _, pm := range members {
		list = append(list, ProtoMemberToMember(*pm))
	}
	return list
}

func (m *Member) LesserThan(m2 *Member) bool {
	return hashInt(m.Addr) < hashInt(m2.Addr)
}

/********************************************************************************
 *				MemberList functions
 ********************************************************************************/
func (ml *MemberList) Contains(member Member) bool {
	ml.lock.RLock()
	defer ml.lock.RUnlock()
	for _, m := range ml.list {
		if m == member {
			return true
		}
	}
	return false
}

func (ml *MemberList) Set(list []Member) {
	ml.lock.Lock()
	defer ml.lock.Unlock()
	ml.list = list
	sort.Slice(ml.list, func(i, j int) bool {
		return hashInt(ml.list[i].Addr) < hashInt(ml.list[j].Addr)
	})
}

func (ml *MemberList) Length() int {
	ml.lock.RLock()
	defer ml.lock.RUnlock()
	return len(ml.list)
}

// Add Member adds a member to the list and returns true
// if member was either inserted / updated
func (ml *MemberList) Add(member Member) bool {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	exists := false
	idx := 0
	for i, m := range ml.list {
		if m.LesserThan(&member) {
			idx = i + 1
		}

		if m.Addr == member.Addr {
			if m.Incarnation != member.Incarnation {
				ml.list[i].Incarnation = member.Incarnation
				log.Debugf("UPDATEMEMBER: Member %v joining with new Incarnation", member)
				return true
			}
			exists = true
			break
		}
	}

	if !exists {
		// Insert
		ml.list = append(ml.list[:idx], append([]Member{member}, ml.list[idx:]...)...)
		log.Debugf("ADDMEMBER: New member %v", member)
		return true
	}

	return false
}

// Remove returns true if a member was removed
func (ml *MemberList) Remove(member Member) bool {
	ml.lock.Lock()
	defer ml.lock.Unlock()
	memberIdx := -1
	for i, m := range ml.list {
		if m == member {
			memberIdx = i
			break
		}
	}

	if memberIdx != -1 {
		// Removing member i
		log.Debugf("REMOVEMEMBER: Removed member %v from memberlist", member)
		ml.list = append(ml.list[:memberIdx], ml.list[memberIdx+1:]...)
		return true
	}
	return false
}

func (ml *MemberList) Print() {
	ml.lock.RLock()
	defer ml.lock.RUnlock()
	log.Debug("Memberlist")
	for _, member := range ml.list {
		log.Debug(member)
	}
}

func (ml *MemberList) ProtoMembers() []*ProtoMember {
	ml.lock.RLock()
	defer ml.lock.RUnlock()

	var res []*ProtoMember
	for _, m := range ml.list {
		res = append(res, m.ProtoMember())
	}

	return res
}

/********************************************************************************
*				Client between Memberlist and FileRing
********************************************************************************/
func (ml *MemberList) FindReplicas(sdfsFileName string) []Member {
	ml.lock.RLock()
	defer ml.lock.RUnlock()

	h := hashInt(sdfsFileName)
	numReplicas := Min(len(ml.list), ReplicationFactor)

	// Default to zero. Because in case no replica has ID greater than given file id,
	// we know that the index is actually 0 (Because we are working with rings)
	idx := 0
	for i, m := range ml.list {
		if hashInt(m.Addr) > h {
			idx = i
			break
		}
	}

	log.Debug("Replicas (Total: %v)", numReplicas)
	var result []Member
	for i := 0; i < numReplicas; i++ {
		log.Debug(i, ml.list[(idx+i)%len(ml.list)])
		result = append(result, ml.list[(idx+i)%len(ml.list)])
	}
	log.Debug("FindReplicas Result: ", result)
	return result
}

func (ml *MemberList) GetPrimaryRange(member Member) Range {
	ml.lock.RLock()
	defer ml.lock.RUnlock()

	idx := -1
	for i, m := range ml.list {
		log.Info(m)
		if m == member {
			idx = i
			break
		}
	}

	if idx >= 0 {
		startIdx := (idx - 1 + len(ml.list)) % len(ml.list)
		return Range{(hashInt(ml.list[startIdx].Addr) + 1) % M, hashInt(ml.list[idx].Addr), false}
	} else {
		return Range{-1, -1, true}
	}
}

func MemberRangeDifference(newRange, oldRange Range) Range {
	// Old end will be equal to new end
	if newRange.start == oldRange.start || newRange.empty || oldRange.empty {
		return Range{empty: true}
	}

	if InRange(oldRange.start, newRange.start, newRange.end, M) {
		return Range{newRange.start, (oldRange.start + M - 1) % M, false}
	}
	return Range{empty: true}
}

func (ml *MemberList) GetRange(member Member) (int64, int64, error) {
	ml.lock.RLock()
	defer ml.lock.RUnlock()

	idx := -1
	for i, m := range ml.list {
		log.Info(m)
		if m == member {
			idx = i
			break
		}
	}

	if idx >= 0 {
		numReplicas := Min(len(ml.list), ReplicationFactor)
		startIdx := (idx - numReplicas + len(ml.list)) % len(ml.list)
		return (hashInt(ml.list[startIdx].Addr) + 1) % M, hashInt(ml.list[idx].Addr), nil
	} else {
		return -1, -1, errors.New("unable to find member in list")
	}
}

func (ml *MemberList) PrintMembers() {
	ml.lock.RLock()
	defer ml.lock.RUnlock()

	fmt.Println("All members:")
	for i, m := range ml.list {
		fmt.Printf("%v)\t%v\t\tId: %v\n", i+1, m.Addr, hashInt(m.Addr))
	}
}

func (ml *MemberList) LogPrintMembers() {
	ml.lock.RLock()
	defer ml.lock.RUnlock()

	log.Debugf("All members:")
	for i, m := range ml.list {
		log.Debugf("%v)\t%v\t\t| RPCAddr: %v \n", i+1, m.Addr, m.RPCAddr)
	}
}

/**********************************************************************************************
*	Helper functions - Used by other modules (Failure Detector / Broadcaster)
***********************************************************************************************/
func (ml *MemberList) Successors(member Member, n int) []Member {
	ml.lock.RLock()
	defer ml.lock.RUnlock()
	n = Min(n, len(ml.list)-1)

	idx := -1
	for i, m := range ml.list {
		if m == member {
			idx = i
			break
		}
	}

	var result []Member
	if idx >= 0 {
		idx++
		for i := 0; i < n; i++ {
			result = append(result, ml.list[(idx+i)%len(ml.list)])
		}
	}
	return result
}

// RandomMembers returns `b` random members from the member list excluding member `exclude`
// Returns all members if fewer than b members in list
func (ml *MemberList) RandomMembers(exclude Member, b int) []Member {
	ml.lock.RLock()
	defer ml.lock.RUnlock()

	b = Min(b, len(ml.list)-1)
	perm := rand.Perm(len(ml.list))

	var result []Member

	for i := 0; i < len(ml.list); i++ {
		if ml.list[perm[i]] == exclude {
			continue
		}

		result = append(result, ml.list[perm[i]])
		if len(result) == b {
			break
		}
	}

	return result
}
