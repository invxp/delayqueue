# Delay Queue

### 单层时间轮 基于Go实现, 主要能力:

1. Push(delaySecond int64, taskType string) *Task
2. AfterFunc(delaySecond int64, f func())

### 具体介绍:

* 支持持久化的单层时间轮
* 通过 [lock-free](https://github.com/golang-design/lockfree) 栈链存储任务
* 通过 [pebble](https://github.com/cockroachdb/pebble) 进行数据持久化

## 如何使用

#### 下载

```
go get github.com/invxp/delayqueue
```

#### 简单示例:

```go
package main

import (
	"github.com/invxp/delayqueue"
	"log"
	"time"
)

func main() {
	//新建一个DelayQueue
	dq := delayqueue.New()

	delaySeconds := int64(5)
	
	//5秒后执行
	dq.AfterFunc(delaySeconds, func() { log.Println("After 5 second function") })

	//开启任务(会阻塞, 创建一个协程)
	go func() {
		dq.Run()
	}()

	//等待10秒后退出
	time.Sleep(time.Second * 10)

	log.Println("close delay queue error", dq.Close())
}

```

#### 测试用例可以这样做:

```
$ go test -v -race -run @XXXXX(具体方法名)
PASS / FAILED
```

#### 或测试全部用例:
```
$ go test -v -race
```

## TODO
1. 优化过期的Task处理方式(多层时间轮, 感觉没啥必要)
2. 支持分布式数据同步(Raft, CP, 放弃性能保证一致性)
