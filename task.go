package delayqueue

import (
	"container/list"
	"sync"
	"time"
)

type wheelTask struct {
	mu   *sync.Mutex
	list *list.List
}

type task struct {
	//任务ID便于持久化
	ID string
	//任务类型
	Type string
	//任务在时间轮上的循环次数，等于0时，执行该任务
	CycleCount int
	//任务在时间轮上的位置
	WheelPosition int
	//任务具体执行
	f func()
}

func (dq *DelayQueue) loadTasks() {
	tasks := dq.GetList()
	if tasks != nil && len(tasks) > 0 {
		for _, t := range tasks {
			delaySeconds := ((t.CycleCount + 1) * int(dq.wheelSize)) + t.WheelPosition
			if delaySeconds > 0 {
				dq.internalPush(time.Duration(delaySeconds)*time.Second, t.ID, t.Type,false)
			}else{
				dq.mu.RLock()
				if f, ok := dq.fun[t.Type]; ok {
					go f()
				}
				dq.mu.RUnlock()
			}
		}
	}
}

func (dq *DelayQueue) internalPush(delaySeconds time.Duration, taskID, taskType string, enableStore bool) {
	dq.mu.RLock()
	f := dq.fun[taskType]
	dq.mu.RUnlock()
	if f == nil {
		return
	}

	if int(delaySeconds.Seconds()) <= 0 {
		go f()
		return
	}
	//从当前时间指针处开始计时
	calculateValue := int(dq.currentIndex) + int(delaySeconds.Seconds())

	cycle := calculateValue / int(dq.wheelSize)
	if cycle > 0 {
		cycle--
	}
	index := calculateValue % int(dq.wheelSize)

	t := &task{
		ID:            taskID,
		Type:          taskType,
		CycleCount:    cycle,
		WheelPosition: index,
		f: f,
	}

	dq.wheels[index].mu.Lock()
	defer dq.wheels[index].mu.Unlock()
	dq.wheels[index].list.PushBack(t)

	if enableStore {
		//持久化任务
		dq.Save(t)
	}
}

func (dq *DelayQueue) scheduleTasks(wheel wheelTask) {
	wheel.mu.Lock()
	defer wheel.mu.Unlock()

	for {
		element := wheel.list.Front()
		if element == nil {
			break
		}

		wheel.list.Remove(element)

		t, ok := element.Value.(*task)

		if !ok {
			continue
		}

		if t.CycleCount == 0 {
			go t.f()
			_ = dq.Delete(t.ID)
		} else {
			t.CycleCount--
		}

	}
}