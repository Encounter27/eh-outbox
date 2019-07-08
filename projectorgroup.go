package outbox

import (
	"fmt"
	"sync"
)

// ReadProjectorGroup is collection of iWorker
type ReadProjectorGroup struct {
	readsideProjectors            map[projectorID]iWorker
	readsideProjectorsFactoriesMu sync.RWMutex
}

var singletonRPG *ReadProjectorGroup

// GetReadProjectorGroup get the singleton object of ReadProjectorGroup
func GetReadProjectorGroup() *ReadProjectorGroup {
	if singletonRPG == nil {
		singletonRPG = new(ReadProjectorGroup)
		singletonRPG.readsideProjectors = make(map[projectorID]iWorker)
	}

	return singletonRPG
}

// Start all the projector worker
func (rpg *ReadProjectorGroup) Start() {
	for _, v := range rpg.readsideProjectors {
		go v.start() // can be async
	}

	go singletonRPG.setStateToAllProjectors(Running)
}

func (rpg *ReadProjectorGroup) setStateToAllProjectors(state int) {
	for _, v := range rpg.readsideProjectors {
		go v.setState(state) // can be async
	}
}

// RegisterProjector to register a new readside projector
func (rpg *ReadProjectorGroup) RegisterProjector(p IProjector) {
	name := projectorID(p.Name())
	id := p.Bit()
	if id == 0 || id > (1<<30) {
		panic(fmt.Sprintf("Projector must have a non zero integer id in the the form of 2^x where x varies from 0 to 30. id = %d", id))
	}

	rpg.readsideProjectorsFactoriesMu.Lock()
	defer rpg.readsideProjectorsFactoriesMu.Unlock()

	if _, ok := rpg.readsideProjectors[name]; ok {
		panic(fmt.Sprintf("Registering duplicate projector for %q", id))
	}

	w := newWorker(p)
	rpg.readsideProjectors[name] = w
}
