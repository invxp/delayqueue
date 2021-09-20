package delayqueue

import (
	"github.com/golang-design/lockfree"
	"log"
	"time"
)

type taskList struct {
	stack *lockfree.Stack
}

type Task struct {
	ID    string //任务ID便于持久化
	Type  string //任务类型
	Cycle int64  //任务在时间轮上的循环次数，等于0时，执行该任务
	Pos   int64  //任务在时间轮上的位置
	f     func() //任务具体执行
}

func (dq *DelayQueue) loadPersistentData() {
	now := time.Now().Unix()

	tm := dq.loadTime()
	if tm == 0 {
		tm = now
	}
	pos, cycle := dq.loadTick(now - tm)

	tasks := dq.loadTasks()

	for _, t := range tasks {
		delaySeconds := (t.Cycle-cycle)*dq.wheelSize + t.Pos - pos
		dq.push(delaySeconds, t.ID, t.Type, nil, false)
	}
}

func (dq *DelayQueue) push(delaySeconds int64, taskID, taskType string, f func(), persistentData bool) {
	if f == nil {
		fun, ok := dq.fun.GetStringKey(taskType)
		if !ok {
			return
		}
		f, ok = fun.(func())
		if !ok {
			return
		}
	}

	if delaySeconds <= 0 {
		go func() {
			log.Println("run old task:", taskID)
			f()
			if persistentData {
				dq.dropTask(taskID)
			}
		}()
		return
	}

	calculateValue, cycle := dq.loadTick(0)

	calculateValue += delaySeconds

	cycle = (calculateValue / dq.wheelSize) - cycle

	tick := calculateValue % dq.wheelSize

	t := &Task{ID: taskID, Type: taskType, Cycle: cycle, Pos: tick, f: f}

	dq.wheels[tick].stack.Push(t)

	log.Printf("store task: %s, type: %s, cycle: %d, pos: %d", taskID, taskType, cycle, tick)

	if persistentData {
		//持久化任务
		dq.saveTask(t)
	}
}

func (dq *DelayQueue) scheduleTasks(wheel taskList, cycle int64) {
	var refillTasks []*Task
	for {
		element := wheel.stack.Pop()
		if element == nil {
			break
		}
		t, ok := element.(*Task)
		if !ok {
			continue
		}

		t.Cycle -= cycle + 1

		if t.Cycle <= 0 {
			go func(f func(), ID string) {
				f()
				dq.dropTask(ID)
			}(t.f, t.ID)
			continue
		}

		dq.saveTask(t)
		refillTasks = append(refillTasks, t)
	}

	for _, task := range refillTasks {
		wheel.stack.Push(task)
	}
}
