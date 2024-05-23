package tracker

import (
	"sync"

	"github.com/google/uuid"

	"github.com/numaproj/numaflow/pkg/isb"
)

type Tracker struct {
	requestMap sync.Map
	// TODO(stream): check if this will be inefficient, and there is a better way for this
	responseIdx sync.Map
}

func NewTracker() *Tracker {
	return &Tracker{
		requestMap:  sync.Map{},
		responseIdx: sync.Map{},
	}
}

func GetNewId() string {
	id, _ := uuid.NewUUID()
	return id.String()
}

func (t *Tracker) AddRequest(msg *isb.ReadMessage) string {
	id := GetNewId()
	//t.requestMap.Store(id, msg)
	return id
}

func (t *Tracker) GetRequest(id string) (*isb.ReadMessage, bool) {
	val, ok := t.requestMap.Load(id)
	return val.(*isb.ReadMessage), ok
}

func (t *Tracker) NewResponse(id string) {
	t.responseIdx.Store(id, 1)
}

func (t *Tracker) IncrementRespIdx(id string) bool {
	idx, ok := t.responseIdx.Load(id)
	if !ok {
		return ok
	}
	newIdx := idx.(int) + 1
	t.responseIdx.Store(id, newIdx)
	return true
}

func (t *Tracker) GetIdx(id string) (int, bool) {
	idx, ok := t.responseIdx.Load(id)
	if !ok {
		return -1, ok
	}
	return idx.(int), ok
}

func (t *Tracker) RemoveRequest(id string) {
	t.requestMap.Delete(id)
	t.responseIdx.Delete(id)
}
