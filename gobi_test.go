package gobi

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

func TestJobSave(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})

	q := NewQueue("gobi", client, QueueOptions{})

	job := q.CreateJob(map[string]int{"foo": 4}, JobOptions{})

	if _, err := job.Save(); err != nil {
		t.Error("failed to save job: ", err)
	}
}

func TestJobProcess(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6383",
		Password: "",
		DB:       0,
	})

	q := NewQueue("customRuleEvent", client, QueueOptions{})

	q.Process(1, func(job Job) (interface{}, error) {
		data, err := job.ToData()
		fmt.Println(job.ID(), data)
		return nil, err
	})

	for {
		time.Sleep(20000)
	}
}
