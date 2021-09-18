package delayqueue

import (
	"container/list"
	"github.com/google/uuid"
	"sync"
	"time"
)

type DelayQueue struct {
	wheelSize 	 uint32
	wheels       []wheelTask
	currentIndex uint32
	mu			 *sync.RWMutex
	fun          map[string]func()
}

func New(wheelSize uint32) *DelayQueue {
	dq := &DelayQueue{wheelSize: wheelSize, currentIndex: 0, mu: &sync.RWMutex{}, fun: make(map[string]func())}
	for i:=uint32(0); i< wheelSize; i++ {
		dq.wheels = append(dq.wheels, wheelTask{&sync.Mutex{}, list.New()})
	}
	return dq
}

func (dq *DelayQueue) Bind(taskType string, f func()) {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	dq.fun[taskType] = f
}


func (dq *DelayQueue) Run() {
	dq.loadTasks()
	for {
		select {
		case <-time.After(time.Second):
			currentIndex := dq.updateCurrentIndex()
			dq.scheduleTasks(dq.wheels[currentIndex])
		}
	}
}

func (dq *DelayQueue) Push(delaySeconds time.Duration, taskType string) {
	dq.internalPush(delaySeconds, uuid.NewString(), taskType, true)
}