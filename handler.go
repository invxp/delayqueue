package delayqueue

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/cockroachdb/pebble"
	"log"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

type HandlerOptions func(*DelayQueue)

func WithWheelSize(wheelSize int64) HandlerOptions {
	return func(dq *DelayQueue) {
		dq.wheelSize = wheelSize
	}
}

func WithFunctions(functions map[string]func()) HandlerOptions {
	return func(dq *DelayQueue) {
		for k, v := range functions {
			dq.fun.Set(k, v)
		}
	}
}

func WithSaveTaskHandler(saveTaskHandler func(taskID, taskType string, timestamp, cycle, pos int64)) HandlerOptions {
	return func(dq *DelayQueue) { dq.saveTaskHandler = saveTaskHandler }
}

func WithLoadTaskHandler(loadTaskHandler func() []Task) HandlerOptions {
	return func(dq *DelayQueue) { dq.loadTaskHandler = loadTaskHandler }
}

func WithDropTaskHandler(dropTaskHandler func(taskID string)) HandlerOptions {
	return func(dq *DelayQueue) { dq.dropTaskHandler = dropTaskHandler }
}

func WithSaveTickHandler(saveTickHandler func(tick int64)) HandlerOptions {
	return func(dq *DelayQueue) { dq.saveTickHandler = saveTickHandler }
}

func WithLoadTickHandler(loadTickHandler func() (tick int64)) HandlerOptions {
	return func(dq *DelayQueue) { dq.loadTickHandler = loadTickHandler }
}

func WithSaveTimeHandler(saveTimeHandler func(timeSecond int64)) HandlerOptions {
	return func(dq *DelayQueue) { dq.saveTimeHandler = saveTimeHandler }
}

func WithLoadTimeHandler(loadTimeHandler func() (timeSecond int64)) HandlerOptions {
	return func(dq *DelayQueue) { dq.loadTimeHandler = loadTimeHandler }
}

var defaultWheelSize = int64(60 * 60 * 24)

var defaultTaskKey = "***m_task****"
var defaultTimeKey = "***a_time***"
var defaultTickKey = "***a_tick***"

var defaultStorageDirectory = "delay_queue"
var defaultDatabase *pebble.DB
var defaultOnce = sync.Once{}

func stringToByte(src string) []byte {
	str := (*reflect.StringHeader)(unsafe.Pointer(&src))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{Data: str.Data, Len: str.Len, Cap: str.Len}))
}

var loadDB = func() {
	defaultOnce.Do(func() {
		var err error
		defaultDatabase, err = pebble.Open(defaultStorageDirectory, &pebble.Options{})
		if err != nil {
			panic(err)
		}
	})
}

var DefaultLoadTaskHandler = func() []Task {
	loadDB()
	var tasks []Task
	iter := defaultDatabase.NewIter(&pebble.IterOptions{LowerBound: stringToByte(defaultTaskKey)})
	for iter.First(); iter.Valid(); iter.Next() {
		task := Task{}
		if err := gob.NewDecoder(bytes.NewReader(iter.Value())).Decode(&task); err != nil {
			panic(err)
		}
		log.Printf("load task: %s, type: %s, cycle: %d, pos: %d, time: %s", task.ID, task.Type, task.Cycle, task.Pos, time.Unix(task.Timestamp, 0).Format("2006-01-02 15:04:05"))
		tasks = append(tasks, task)
	}
	_ = iter.Close()
	return tasks
}

var DefaultSaveTaskHandler = func(taskID, taskType string, timestamp, cycle, pos int64) {
	loadDB()
	var task = Task{taskID, taskType, cycle, pos, timestamp, true, nil}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(task); err != nil {
		panic(err)
	}

	log.Printf("save task: %s, type: %s, cycle: %d, pos: %d, time: %s, error: %v", task.ID, task.Type, task.Cycle, task.Pos, time.Unix(task.Timestamp, 0).Format("2006-01-02 15:04:05"), defaultDatabase.Set(stringToByte(defaultTaskKey+taskID), buf.Bytes(), &pebble.WriteOptions{Sync: true}))
}

var DefaultDropTaskHandler = func(taskID string) {
	loadDB()
	log.Printf("drop task: %s, error: %v", taskID, defaultDatabase.Delete(stringToByte(defaultTaskKey+taskID), &pebble.WriteOptions{Sync: true}))
}

var DefaultSaveTickHandler = func(tick int64) {
	loadDB()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, tick); err != nil {
		panic(err)
	}
	if err := defaultDatabase.Set(stringToByte(defaultTickKey), buf.Bytes(), &pebble.WriteOptions{Sync: true}); err != nil {
		log.Printf("save tick: %d, error: %v", tick, err)
	}
}

var DefaultLoadTickHandler = func() (tick int64) {
	loadDB()
	data, closer, err := defaultDatabase.Get(stringToByte(defaultTickKey))
	if err != nil {
		log.Printf("load tick error: %v", err)
		return 0
	}
	defer func() {
		_ = closer.Close()
	}()
	err = binary.Read(bytes.NewReader(data), binary.BigEndian, &tick)
	if err != nil {
		log.Printf("load tick error: %v", err)
	}
	return
}

var DefaultSaveTimeHandler = func(time int64) {
	loadDB()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, time); err != nil {
		panic(err)
	}
	if err := defaultDatabase.Set(stringToByte(defaultTimeKey), buf.Bytes(), &pebble.WriteOptions{Sync: true}); err != nil {
		log.Printf("save time: %d, error: %v", time, err)
	}
}

var DefaultLoadTimeHandler = func() (time int64) {
	loadDB()
	data, closer, err := defaultDatabase.Get(stringToByte(defaultTimeKey))
	if err != nil {
		log.Printf("load time error: %v", err)
		return 0
	}
	defer func() {
		_ = closer.Close()
	}()
	err = binary.Read(bytes.NewReader(data), binary.BigEndian, &time)
	if err != nil {
		log.Printf("load time error: %v", err)
	}
	return
}
