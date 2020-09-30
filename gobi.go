package gobi

import (
	"encoding/json"
)

type JobOptions struct {
	Timestamp   int64          `json:"timestamp"`
	Delay       int64          `json:"delay"`
	Timeout     int64          `json:"timeout"`
	Retries     int64          `json:"retries"`
	Backoff     BackoffOptions `json:"backoff"`
	StackTraces []string       `json:"stacktraces"`
}

type BackoffOptions struct {
	Strategy string `json:"strategy"`
	Delay    int64  `json:"delay"`
}

type Job interface {
	Save() (string, error)
	ID() string
	DecrementRetries()
	AddError(err error)
	GetDelay() int64
	ToData() (string, error)
	SetID(string)
}

type JobData struct {
	data    interface{} `json:"data"`
	options JobOptions  `json:"options"`
	status  string      `json:"status"`
}

type job struct {
	queue Queue

	data    interface{}
	options JobOptions
	status  string
	id      string
}

func (j *job) Save() (string, error) {

	data, err := j.ToData()
	if err != nil {
		return "", err
	}

	jobId, err := j.queue.RunScriptForName(
		"addJob",
		[]string{
			j.queue.ToKey("id"),
			j.queue.ToKey("jobs"),
			j.queue.ToKey("waiting"),
		},
		"", // Not supporting nullable specific Job IDs out of the box
		data,
	)

	return jobId.(string), err
}

func (j *job) ID() string {
	return j.id
}

func (j *job) SetID(id string) {
	j.id = id
}

func (j *job) DecrementRetries() {
	j.options.Retries -= 1
}

func (j *job) AddError(err error) {
	stacktrace := err.Error()
	j.options.StackTraces = append([]string{stacktrace}, j.options.StackTraces...)
}

func (j *job) GetDelay() int64 {
	if j.options.Retries == 0 {
		return -1
	}

	switch j.options.Backoff.Strategy {
	case "fixed":
		return j.options.Backoff.Delay
	case "exponential":
		j.options.Backoff.Delay *= 2
		return j.options.Backoff.Delay
	case "immediate":
	default:
		return 0
	}

	return 0
}

func (j *job) ToData() (string, error) {
	raw, err := json.Marshal(&JobData{
		status:  j.status,
		data:    j.data,
		options: j.options,
	})

	// It makes me sad to do a re-alloc here, but we need to test if
	// *redis.Script is fine passing down a []byte before we can remove it.
	return string(raw), err
}

func NewJob(status string, options JobOptions, data interface{}, q Queue) Job {
	return &job{
		queue:   q,
		data:    data,
		options: options,
		status:  status,
	}
}
