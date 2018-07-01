package gobi

import (
	"testing"

	"github.com/ttacon/rpp"
)

func TestJobSave(t *testing.T) {
	pool, err := rpp.RPP("redis://127.0.0.1:6379", 5, 5)
	if err != nil {
		t.Error("failed to create rpp: ", err)
		t.Fail()
	}

	q := NewQueue("gobi", pool, QueueOptions{})

	job := q.CreateJob(map[string]int{"foo": 4}, JobOptions{})

	if err := job.Save(); err != nil {
		t.Error("failed to save job: ", err)
	}

}
