package outbox

import (
	"context"
	"fmt"
	"sync"
)

const (
	KafkaProjectorBit         = 1 << 0
	MongoProjectorBit         = 1 << 1
	ElasticsearchProjectorBit = 1 << 2
	RedisProjectorBit         = 1 << 3
	CassandraProjectorBit     = 1 << 4
	PubsubProjectorBit        = 1 << 5
	WebSocketProjectorBit     = 1 << 6
)

// Possible worker states.
const (
	Stopped = 0
	Paused  = 1
	Running = 2
)

type Projection interface {
	Start()
	SetState(state int)
	Worker()
	WriteToReadside(ctx context.Context, key string, data interface{}) error
}

type ProjectorID string

type ReadProjectorGroup struct {
	readsideProjectors            map[ProjectorID]Projection
	readsideProjectorsFactoriesMu sync.RWMutex
}

var singletonRPG *ReadProjectorGroup

func GetReadProjectorGroup() *ReadProjectorGroup {
	if singletonRPG == nil {
		singletonRPG = new(ReadProjectorGroup)
		singletonRPG.readsideProjectors = make(map[ProjectorID]Projection)
	}

	return singletonRPG
}

// Start all the projector worker
func (rpg *ReadProjectorGroup) Start() {
	for _, v := range rpg.readsideProjectors {
		go v.Start() // can be async
	}

	go singletonRPG.setStateToAllProjectors(Running)
}

func (rpg *ReadProjectorGroup) setStateToAllProjectors(state int) {
	for _, v := range rpg.readsideProjectors {
		go v.SetState(state) // can be async
	}
}

func (rpg *ReadProjectorGroup) RegisterProjector(id ProjectorID, p Projection) {
	if id == ProjectorID("") {
		panic("Projector must have a meaningful name.")
	}

	rpg.readsideProjectorsFactoriesMu.Lock()
	defer rpg.readsideProjectorsFactoriesMu.Unlock()

	if _, ok := rpg.readsideProjectors[id]; ok {
		panic(fmt.Sprintf("Registering duplicate projector for %q", id))
	}
	rpg.readsideProjectors[id] = p
}
