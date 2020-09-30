package gobi

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

const BEE_QUEUE_DEFAULT_PREFIX = "bq"

type Queue interface {
	Name() string
	Conn() redis.Conn
	CreateJob(data interface{}, options JobOptions) Job
	ToKey(string) string
	Process(int, func(*Job))

	RunScriptForName(name string, keys []string, args ...interface{}) (interface{}, error)
}

type queue struct {
	name        string
	client      *redis.Client
	queuePrefix string
	scripts     map[string]*redis.Script
	jobs        map[string]*Job
}

type QueueOptions struct {
	QueuePrefix string
}

type QueueEvent struct {
	event string
	data  interface{}
	id    string
}

type scriptInfo struct {
	numKeys int
	raw     string
}

var scriptsToEnsure = map[string]scriptInfo{
	"addJob": scriptInfo{3, addJob},
}

func NewQueue(name string, client *redis.Client, options QueueOptions) Queue {
	// Setup our queue.
	q := &queue{
		name,
		client,
		options.QueuePrefix,
		map[string]*redis.Script{},
	}

	if len(q.queuePrefix) == 0 {
		q.queuePrefix = BEE_QUEUE_DEFAULT_PREFIX
	}

	q.ensureScripts()
	q.subscribeToMessages()
	return q
}

func timestampInMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (q *queue) Name() string {
	return q.name
}

func (q *queue) CreateJob(data interface{}, options JobOptions) Job {

	if options.Timestamp == 0 {
		options.Timestamp = timestampInMillis()
	}

	return &job{
		queue:   q,
		data:    data,
		options: options,
		status:  "created",
	}
}

func (q *queue) ToKey(s string) string {
	return fmt.Sprintf("%s:%s:%s", q.queuePrefix, q.name, s)
}

func (q *queue) Conn() redis.Conn {
	return q.client.Conn()
}

func (q *queue) Process(concurrency int, handler func(*Job)) {

}

func (q *queue) ensureScripts() {
	for key, rawScript := range scriptsToEnsure {
		newScript := redis.NewScript(rawScript.numKeys, rawScript.raw)
		q.scripts[key] = newScript

		// Currently, `redis.NewScript` will not automatically
		// ensure that our script exists in the Redis but rather
		// it ensures that it exists at execution time. For now,
		// as we only provide Go consumer code for bee-queue, this
		// is acceptable (as the script execution will detect that
		// the script was already loaded into Redis by node).
	}
}

func (q *queue) RunScriptForName(name string) (*redis.Script, bool) {
	script, ok := q.scripts[name]
	if !ok {
		return errors.New("no such script")
	}

	return script, ok
}

func (q *queue) subsribeToMessages() {
	pubSub := q.client.Subscribe(q.ToKey("events"))
	messageChannel := pubSub.Channel()

	go func() {
		defer pubSub.Close()
		for message := range messageChannel {
			var event QueueEvent
			json.Unmarshal(message.Payload, &event)

			job := q.jobs[event.id]
			if job == nil {
				continue
			}

			switch event.event {
			case "progress":
				job.SetProgress(event.data)
			case "progress":
				job.DecrementRetries()
			case "succeeded":
			case "failed":
				delete(q.jobs, event.id)
			}

		}
	}()
}

func (q *queue) finishJob(jobError error, data interface{}, j *Job) error {
	status := "succeeded"
	if jobError {
		status = "failed"
	}

	pipeline := q.client.TxPipeline()
	pipeline.LRem(q.ToKey("active"), 0, j.Id())
	pipeline.SRem(q.ToKey("stalling"), j.Id())
	_, err := pipeline.Exec()
	if err != nil {
		return err
	}

}
