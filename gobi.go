package gobi

import (
	"encoding/json"
	"errors"
	"fmt"
)

type JobOptions struct {
	Timestamp int64          `json:"timestamp"`
	Delay     int            `json:"delay"`
	Timeout   int            `json:"timeout"`
	Retries   int            `json:"retries"`
	Backoff   BackoffOptions `json:"backoff"`
}

type BackoffOptions struct {
	Strategy string `json:"strategy"`
	Delay    int    `json:"delay"`
}

type Job interface {
	Save() error
	Id() string
}

type job struct {
	queue Queue

	data    interface{}
	options JobOptions
	status  string
	id      string
}

func (j *job) Save() error {
	addJobScript, ok := j.queue.scriptForName("addJob")
	if !ok {
		return errors.New("no such script")
	}

	data, err := j.toData()
	if err != nil {
		return err
	}

	jobId, err := addJobScript.Run(
		j.queue.client,
		[]string{j.queue.ToKey("id"), j.queue.ToKey("jobs"), j.queAddue.ToKey("waiting")},
		"", // Not supporting nullable specific Job IDs out of the box
		data,
	).Result()

	fmt.Println("jobId: ", jobId)

	return err
}

func (j *Job) Id() string {
	return j.id
}

func (j *job) SetProgress(data interface{}) {
	j.data = data
}

func (j *job) DecrementRetries() {
	j.options.Retries -= 1
}

func (j *job) toData() (string, error) {
	raw, err := json.Marshal(map[string]interface{}{
		"status":  j.status,
		"data":    j.data,
		"options": j.options,
	})

	// It makes me sad to do a re-alloc here, but we need to test if
	// *redis.Script is fine passing down a []byte before we can remove it.
	return string(raw), err
}
