package gobi

import (
	"testing"

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

	if err := job.Save(); err != nil {
		t.Error("failed to save job: ", err)
	}
}

func TestJobProcess(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})

	q := NewQueue("gobi", client, QueueOptions{})
	job := q.CreateJob(map[string]int{"foo": 4}, JobOptions{})

	if err := job.Save(); err != nil {
		t.Error("failed to save job: ", err)
	}

	q.Process(1, func(job Job) (interface{}, error) {
		return nil, nil
	})
}
