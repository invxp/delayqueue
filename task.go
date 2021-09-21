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
	ID               string //任务ID便于持久化
	Type             string //任务类型
	Cycle            int64  //任务在时间轮上的循环次数，等于0时，执行该任务
	Pos              int64  //任务在时间轮上的位置
	Timestamp        int64  //任务首次加入时间
	enablePersistent bool   //任务是否需要持久化
	f                func() //任务具体执行
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
		f := dq.searchFunction(t.Type)
		if dq.ensureValidTask(delaySeconds, t.Timestamp, t.ID, t.Type, f) {
			dq.saveTask(dq.push(delaySeconds, t.Timestamp, t.ID, t.Type, f, true))
		} else {
			dq.dropTask(t.ID)
		}
	}
}

func (dq *DelayQueue) searchFunction(taskType string) func() {
	if fun, ok := dq.fun.GetStringKey(taskType); ok {
		return fun.(func())
	}
	return func() {}
}

func (dq *DelayQueue) ensureValidTask(delaySeconds, timestamp int64, taskID, taskType string, f func()) (isValidTask bool) {
	if delaySeconds <= 0 || (time.Now().Unix()-timestamp) > delaySeconds {
		go func() {
			log.Printf("exec task: %s, type: %s, pos: %d, time: %s", taskID, taskType, delaySeconds, time.Unix(timestamp, 0).Format("2006-01-02 15:04:05"))
			f()
		}()
		return false
	}
	return true
}

func (dq *DelayQueue) push(delaySeconds, timestamp int64, taskID, taskType string, f func(), enablePersistent bool) *Task {
	calculateValue, cycle := dq.loadTick(0)

	calculateValue += delaySeconds

	cycle = (calculateValue / dq.wheelSize) - cycle

	tick := calculateValue % dq.wheelSize

	t := &Task{ID: taskID, Type: taskType, Cycle: cycle, Pos: tick, Timestamp: timestamp, f: f, enablePersistent: enablePersistent}

	dq.wheels[tick].stack.Push(t)

	log.Printf("push task: %s, type: %s, cycle: %d, pos: %d, time: %s", taskID, taskType, cycle, tick, time.Unix(timestamp, 0).Format("2006-01-02 15:04:05"))

	return t
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
				log.Printf("exec task: %s, type: %s, pos: %d, time: %s", t.ID, t.Type, t.Pos, time.Unix(t.Timestamp, 0).Format("2006-01-02 15:04:05"))
				f()
				if t.enablePersistent {
					dq.dropTask(ID)
				}
			}(t.f, t.ID)
			continue
		}

		refillTasks = append(refillTasks, t)

		if t.enablePersistent {
			dq.saveTask(t)
		}
	}

	//TODO 方式不够优雅, 拿出来发现时间不够又重新放回去了
	//TODO 目前没想到更好的解决方式
	//TODO 用多级时间轮能减少IO, 但是是否真的有必要?
	for _, task := range refillTasks {
		wheel.stack.Push(task)
	}
}
