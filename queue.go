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
	CreateJob(data interface{}, options JobOptions) Job
	ToKey(string) string
	Process(int, func(Job))

	RunScriptForName(name string, keys []string, args ...interface{}) (interface{}, error)
}

type queue struct {
	name            string
	client          *redis.Client
	queuePrefix     string
	scripts         map[string]*redis.Script
	jobs            map[string]Job
	removeOnFailure bool
}

type QueueOptions struct {
	QueuePrefix     string
	RemoveOnFailure bool
}

type QueueEvent struct {
	event string
	data  interface{}
	id    string
}

type scriptInfo struct {
	raw string
}

var scriptsToEnsure = map[string]scriptInfo{
	"addJob": scriptInfo{addJob},
}

func NewQueue(name string, client *redis.Client, options QueueOptions) Queue {
	// Setup our queue.
	q := &queue{
		name,
		client,
		options.QueuePrefix,
		map[string]*redis.Script{},
		map[string]Job{},
		options.RemoveOnFailure,
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

	options.StackTraces = make([]string, 0)

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

func (q *queue) RunScriptForName(name string, keys []string, args ...interface{}) (interface{}, error) {
	script, ok := q.scripts[name]
	if !ok {
		return nil, errors.New("no such script")
	}

	result, err := script.Run(q.client, keys, args).Result()
	return result, err
}

func (q *queue) Process(concurrency int, handler func(Job)) {

}

func (q *queue) ensureScripts() {
	for key, rawScript := range scriptsToEnsure {
		newScript := redis.NewScript(rawScript.raw)
		q.scripts[key] = newScript

		// Currently, `redis.NewScript` will not automatically
		// ensure that our script exists in the Redis but rather
		// it ensures that it exists at execution time. For now,
		// as we only proviDe Go consumer code for bee-queue, this
		// is acceptable (as the script execution will detect that
		// the script was already loaded into Redis by node).
	}
}

func (q *queue) subscribeToMessages() {
	pubSub := q.client.Subscribe(q.ToKey("events"))
	messageChannel := pubSub.Channel()

	go func() {
		defer pubSub.Close()
		for message := range messageChannel {
			var event QueueEvent
			json.Unmarshal([]byte(message.Payload), &event)

			job, ok := q.jobs[event.id]
			if !ok {
				continue
			}

			switch event.event {
			case "progress":
				job.SetProgress(event.data)
			case "retrying":
				job.DecrementRetries()
			case "succeeded":
			case "failed":
				delete(q.jobs, event.id)
			}

		}
	}()
}

func (q *queue) finishJob(jobError error, data interface{}, j Job) error {
	var status string
	if jobError != nil {
		status = "failed"
	}

	pipeline := q.client.TxPipeline()
	pipeline.LRem(q.ToKey("active"), 0, j.ID())
	pipeline.SRem(q.ToKey("stalling"), j.ID())

	_, err := pipeline.Exec()
	if err != nil {
		return err
	}

	if jobError == nil {
		status = "succeeded"
	} else {
		status = "failed"
		j.AddError(err)
		delay := j.GetDelay()

		if delay < 0 {
			if q.removeOnFailure {
				pipeline.HDel(q.ToKey("jobs"), j.ID())
			} else {
				jobData, err := j.ToData()
				if err != nil {
					return err
				}

				pipeline.HSet(q.ToKey("jobs"), j.ID(), jobData)
				pipeline.SAdd(q.ToKey("failed"), j.ID())
			}
		} else {
			j.DecrementRetries()
			status = "retrying"
		}
	}

	event := &QueueEvent{
		event: status,
		id:    j.ID(),
		data:  data,
	}

	return nil
}
