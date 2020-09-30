package gobi

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/go-redis/redis"
)

const BEE_QUEUE_DEFAULT_PREFIX = "bq"

type Queue interface {
	Name() string
	CreateJob(data interface{}, options JobOptions) Job
	ToKey(string) string
	Process(int, func(Job) (interface{}, error))

	RunScriptForName(name string, keys []string, args ...interface{}) (interface{}, error)
	NewJobFromId(string) (Job, error)
}

type queue struct {
	name            string
	client          *redis.Client
	queuePrefix     string
	scripts         map[string]*redis.Script
	removeOnFailure bool
	removeOnSuccess bool
}

type QueueOptions struct {
	QueuePrefix     string
	RemoveOnFailure bool
	RemoveOnSuccess bool
}

type QueueEvent struct {
	event string      `json:"event"`
	data  interface{} `json:"data"`
	id    string      `json:"id"`
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
		options.RemoveOnFailure,
		options.RemoveOnSuccess,
	}

	if len(q.queuePrefix) == 0 {
		q.queuePrefix = BEE_QUEUE_DEFAULT_PREFIX
	}

	q.ensureScripts()

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

	return NewJob("created", options, data, q)
}

func (q *queue) ToKey(s string) string {
	return fmt.Sprintf("%s:%s:%s", q.queuePrefix, q.name, s)
}

func (q *queue) RunScriptForName(name string, keys []string, args ...interface{}) (interface{}, error) {
	script, ok := q.scripts[name]
	if !ok {
		return nil, errors.New("no such script")
	}

	result, err := script.Run(q.client, keys, args...).Result()
	return result, err
}

func (q *queue) Process(concurrency int, handler func(Job) (interface{}, error)) {
	pool := workerpool.New(concurrency)

	// Start one worker process for each concurrency
	for i := 0; i < concurrency; i++ {
		pool.Submit(func() {
			for {
				// TODO: We should handle this error. Maybe using a channel of some kind?
				job, err := q.waitForJob()
				if err != nil {
					fmt.Println(err)
					continue
				}
				data, err := handler(job)
				q.finishJob(err, data, job)
			}
		})
	}
}

func (q *queue) waitForJob() (Job, error) {
	jobId, err := q.client.BRPopLPush(q.ToKey("waiting"), q.ToKey("active"), time.Duration(0)).Result()
	if err != nil {
		return nil, err
	}
	job, err := q.NewJobFromId(jobId)
	return job, err
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

	jobData, err := j.ToData()
	if err != nil {
		return err
	}

	if jobError == nil {
		status = "succeeded"
		if q.removeOnSuccess {
			pipeline.HDel(q.ToKey("jobs"), j.ID())
		} else {
			pipeline.HSet(q.ToKey("jobs"), j.ID(), jobData)
			pipeline.SAdd(q.ToKey("succeeded"), j.ID())
		}
	} else {
		status = "failed"
		j.AddError(err)
		delay := j.GetDelay()

		if delay < 0 {
			if q.removeOnFailure {
				pipeline.HDel(q.ToKey("jobs"), j.ID())
			} else {
				pipeline.HSet(q.ToKey("jobs"), j.ID(), jobData)
				pipeline.SAdd(q.ToKey("failed"), j.ID())
			}
		} else {
			j.DecrementRetries()
			status = "retrying"
			pipeline.HSet(q.ToKey("jobs"), j.ID(), jobData)

			if delay == 0 {
				pipeline.LPush(q.ToKey("waiting"), j.ID())
			} else {
				time := time.Now().Unix() + delay
				pipeline.ZAdd(q.ToKey("delayed"), redis.Z{Score: float64(time), Member: j.ID()})
			}
		}
	}

	event, err := json.Marshal(&QueueEvent{
		event: status,
		id:    j.ID(),
		data:  data,
	})
	if err != nil {
		return err
	}

	pipeline.Publish(q.ToKey("events"), event)
	_, err = pipeline.Exec()

	return err
}

func (q *queue) NewJobFromId(jobId string) (Job, error) {
	raw, err := q.client.HGet(q.ToKey("jobs"), jobId).Result()

	if err != nil {
		return nil, err
	}

	var jobData JobData
	json.Unmarshal([]byte(raw), &jobData)
	job := NewJob(jobData.Status, jobData.Options, jobData.Data, q)

	job.SetID(jobId)

	return job, nil
}
