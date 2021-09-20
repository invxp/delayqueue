# Delay Queue

### 单层时间轮 基于Go实现, 主要能力:

1. Push(delaySecond int64, taskType string) []values
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
	"fmt"
	"github.com/invxp/delayqueue"
	"time"
)

func main() {
    dq := delayqueue.New(10)

    dq.AfterFunc(1, func() { fmt.Println("After 1 second function") })

    go func() {
        dq.Run()
    }()

    time.Sleep(time.Second * 5)

    dq.Close()
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
1. 优化过期的Task处理方式
2. 支持分布式数据同步(Raft)
