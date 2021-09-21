package delayqueue

import "fmt"

//loadTick 获取当前的时间论位置
//deltaTime - 本次时间与上次时间的间隔
func (dq *DelayQueue) loadTick(deltaTime int64) (pos int64, cycle int64) {
	tick := dq.loadTickHandler()
	if tick >= dq.wheelSize {
		//如果tick >= 轮盘大小, 则为严重错误 (理论上不应该存在)
		panic(fmt.Sprintf("tick greater than wheel size: %d >= %d", tick, dq.wheelSize))
	}

	//每次轮盘位置++
	//如果超过轮盘大小则取模计算实际位置
	//如果上次启动的时间和本次时间间隔太大, 则计算实际需要走动的数量
	pos = tick + deltaTime + 1
	cycle = pos / dq.wheelSize
	if pos >= dq.wheelSize {
		pos %= dq.wheelSize
	}
	return pos, cycle
}

func (dq *DelayQueue) saveTask(task *Task) {
	dq.saveTaskHandler(task.ID, task.Type, task.Timestamp, task.Cycle, task.Pos)
}

func (dq *DelayQueue) loadTasks() []Task {
	return dq.loadTaskHandler()
}

func (dq *DelayQueue) dropTask(taskID string) {
	dq.dropTaskHandler(taskID)
}

func (dq *DelayQueue) loadTime() int64 {
	return dq.loadTimeHandler()
}

func (dq *DelayQueue) saveTime(time int64) {
	dq.saveTimeHandler(time)
}
