package delayqueue

import (
	"testing"
	"time"
)

func TestRunDelayQueue(t *testing.T) {
	queue := New(60 * 60 * 24)

	go queue.Run()

	time.Sleep(time.Second)

	queue.Push(time.Second, "MU")

	c := make(chan struct{})
	c <- struct{}{}
}
