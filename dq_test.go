package delayqueue

import (
	"fmt"
	"github.com/cornelk/hashmap"
	"log"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunDelayQueue(t *testing.T) {
	dq := New(10)

	dq.AfterFunc(1, func() { fmt.Println("After 1 second function") })

	go func() {
		dq.Run()
	}()

	time.Sleep(time.Second * 5)

	dq.Close()
}

func TestRunDelayQueueWithFunction(t *testing.T) {
	dq := New(10, WithFunctions(map[string]func(){"MU": func() {
		fmt.Println("MU Function")
	}}))

	dq.Push(1, "MU")

	go func() {
		dq.Run()
	}()

	time.Sleep(time.Second * 5)

	dq.Close()
}

func TestRunDelayQueueWithBind(t *testing.T) {
	dq := New(10)

	dq.Bind("Bind", func() { fmt.Println("Bind Function") })

	dq.Push(1, "Bind")

	go func() {
		time.Sleep(time.Second * 35)
		dq.Close()
	}()

	dq.Run()
}

func TestCustomPersistentInMemory(t *testing.T) {
	hashMap := hashmap.New(hashmap.DefaultSize)
	wheelSize := rand.Int63n(60)
	ticker := rand.Int63n(wheelSize)
	timeSecond := time.Now().Unix() - rand.Int63n(15)

	hashMap.Set("test_5", Task{"test_5", "MU", 0, 5, nil})
	hashMap.Set("test_15", Task{"test_15", "MU", 1, 5, nil})

	queue := New(wheelSize, WithFunctions(map[string]func(){"MU": func() {}}),
		WithDropTaskHandler(func(taskID string) {
			lastLen := hashMap.Len()
			hashMap.Del(taskID)
			currentLen := hashMap.Len()
			log.Printf("drop task: %s, last: %d, current: %d", taskID, lastLen, currentLen)
		}), WithLoadTaskHandler(func() []Task {
			var task []Task
			for item := range hashMap.Iter() {
				log.Printf("load task: %s, type: %s, cycle: %d, pos: %d", item.Value.(Task).ID, item.Value.(Task).Type, item.Value.(Task).Cycle, item.Value.(Task).Pos)
				task = append(task, item.Value.(Task))
			}
			return task
		}), WithSaveTaskHandler(func(taskID, taskType string, cycle, pos int64) {
			log.Printf("save task: %s, type: %s, cycle: %d, pos: %d", taskID, taskType, cycle, pos)
			hashMap.Set(taskID, Task{taskID, taskType, cycle, pos, nil})
		}), WithLoadTickHandler(func() (tick int64) {
			tick = atomic.LoadInt64(&ticker)
			log.Printf("load tick: %d", tick)
			return tick
		}), WithSaveTickHandler(func(tick int64) {
			atomic.StoreInt64(&ticker, tick)
		}), WithLoadTimeHandler(func() (ts int64) {
			return atomic.LoadInt64(&timeSecond)
		}), WithSaveTimeHandler(func(ts int64) {
			atomic.StoreInt64(&timeSecond, ts)
		}))

	queue.Push(rand.Int63n(30), "MU")

	go func() {
		time.Sleep(time.Second * 30)
		queue.Close()
	}()

	queue.Run()
}
