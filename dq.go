package delayqueue

import (
	"encoding/gob"
	"github.com/cornelk/hashmap"
	"github.com/golang-design/lockfree"
	"github.com/google/uuid"
	"strings"
	"time"
)

type DelayQueue struct {
	wheelSize int64            //轮盘大小, 推荐60 * 60 * 24 (一天走一轮)
	wheels    []taskList       //轮盘数组 (lock-free stack)
	fun       *hashmap.HashMap //不同类型的任务执行不同的方法 (lock-free hashmap)
	close     chan struct{}    //退出信号

	saveTaskHandler func(taskID, taskType string, cycle, pos int64) //保存任务回调
	loadTaskHandler func() []Task                                   //加载任务回调
	dropTaskHandler func(taskID string)                             //删除任务回调
	saveTickHandler func(tick int64)                                //保存轮盘指针回调
	loadTickHandler func() (tick int64)                             //加载轮盘指针回调
	saveTimeHandler func(timeSecond int64)                          //保存时间回调
	loadTimeHandler func() (timeSecond int64)                       //加载时间回调
}

//注册Task对象
func init() {
	gob.Register(Task{})
}

//New 创建一个时间轮, 默认通过Pebble进行数据持久化, 可自定义持久化方式
//wheelSize - 轮盘大小, 推荐60 * 60 * 24 (一天走一轮)
//options 自定义持久化函数
func New(wheelSize int64, options ...HandlerOptions) *DelayQueue {
	dq := &DelayQueue{wheelSize: wheelSize, fun: hashmap.New(hashmap.DefaultSize), close: make(chan struct{})}
	for i := int64(0); i < wheelSize; i++ {
		dq.wheels = append(dq.wheels, taskList{lockfree.NewStack()})
	}

	dq.saveTaskHandler = DefaultSaveTaskHandler
	dq.loadTaskHandler = DefaultLoadTaskHandler
	dq.dropTaskHandler = DefaultDropTaskHandler
	dq.saveTickHandler = DefaultSaveTickHandler
	dq.loadTickHandler = DefaultLoadTickHandler
	dq.saveTimeHandler = DefaultSaveTimeHandler
	dq.loadTimeHandler = DefaultLoadTimeHandler

	for _, opt := range options {
		opt(dq)
	}

	dq.loadPersistentData()

	return dq
}

//Bind 向模块注册一个任务类型绑定其要运行的func
//taskType - 任务类型
//f - 具体执行某个方法
func (dq *DelayQueue) Bind(taskType string, f func()) {
	dq.fun.Set(taskType, f)
}

//Run 执行任务 (阻塞)
func (dq *DelayQueue) Run() {
	for {
		select {
		case <-time.After(time.Second):
			dq.saveTimeHandler(time.Now().Unix())
			tick, cycle := dq.loadTick(0)
			dq.saveTickHandler(tick)
			go func(tick, cycle int64) {
				dq.scheduleTasks(dq.wheels[tick], cycle)
			}(tick, cycle)
		case <-dq.close:
			return
		}
	}
}

//Push 推送一个任务 (持久化)
//delaySeconds - 单位 (n秒后)
//taskType - 任务类型 (与Bind对应, 没有则直接返回)
func (dq *DelayQueue) Push(delaySeconds int64, taskType string) {
	dq.push(delaySeconds, strings.ReplaceAll(uuid.NewString(), "-", ""), taskType, nil, true)
}

//AfterFunc 推送一个任务 (非持久化)
//delaySeconds - 单位 (n秒后)
//f - 具体执行的函数
func (dq *DelayQueue) AfterFunc(delaySeconds int64, f func()) {
	dq.push(delaySeconds, strings.ReplaceAll(uuid.NewString(), "-", ""), "", f, false)
}

//Close 终止
func (dq *DelayQueue) Close() {
	close(dq.close)
	if defaultDatabase != nil {
		_ = defaultDatabase.Close()
	}
}
