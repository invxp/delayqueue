package delayqueue

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
)

//TODO rewrite

func (dq *DelayQueue) updateCurrentIndex() uint32 {
	currentIndex := atomic.AddUint32(&dq.currentIndex, 1)
	if  currentIndex >= dq.wheelSize {
		currentIndex %= dq.wheelSize
		atomic.StoreUint32(&dq.currentIndex, currentIndex)
	}
	return currentIndex
}

func (dq *DelayQueue) Save(task *task) {
	//持久化时，不需要存储链表关系
	tk, err := json.Marshal(task)
	if err != nil {
		panic(err)
	}

	if string(tk) != "" {
		_ = fmt.Sprintf("%s%s", "AA", task.ID)
		/*
		//如果key不存在
		if val, _ := rd.Client.Get(key).Result(); val == "" {
			rd.Client.LPush(rd.TaskListKey, task.ID)
		}
		result := rd.Client.Set(key, string(tk), 0)
		return result.Err()
		 */
	}
}

func (dq *DelayQueue) GetList() []*task {
	/*
	listResult := dq.Client.LRange(dq.TaskListKey, 0, -1)
	listArray, _ := listResult.Result()
	tasks := []*task{}
	if listArray != nil && len(listArray) > 0 {
		for _, item := range listArray {
			key := fmt.Sprintf("%s%s", TASK_KEY_PREFIX, item)
			taskCmd := dq.Client.Get(key)
			if val, err := taskCmd.Result(); err == nil {
				entity := task{}
				err := json.Unmarshal([]byte(val), &entity)
				if err == nil {
					tasks = append(tasks, &entity)
				}
			}
		}
	}

	return tasks
	 */

	return nil
}

func (dq *DelayQueue) Delete(taskId string) error {
	//dq.Client.LRem(dq.TaskListKey, 0, taskId)
	//dq.Client.Del(fmt.Sprintf("%s%s", TASK_KEY_PREFIX, taskId))
	return nil
}