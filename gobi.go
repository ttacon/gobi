package gobi

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

const BEE_QUEUE_DEFAULT_PREFIX = "bq"

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

type Queue interface {
	Name() string
	Conn() redis.Conn
	CreateJob(data interface{}, options JobOptions) Job
	ToKey(string) string

	scriptForName(name string) (*redis.Script, bool)
}

type Job interface {
	Save() error
}

type queue struct {
	name        string
	pool        *redis.Pool
	queuePrefix string
	scripts     map[string]*redis.Script
}

type job struct {
	queue Queue

	data    interface{}
	options JobOptions
	status  string
}

type QueueOptions struct {
	QueuePrefix string
}

func NewQueue(name string, pool *redis.Pool, options QueueOptions) Queue {
	// Setup our queue.
	q := &queue{
		name,
		pool,
		options.QueuePrefix,
		map[string]*redis.Script{},
	}

	if len(q.queuePrefix) == 0 {
		q.queuePrefix = BEE_QUEUE_DEFAULT_PREFIX
	}

	q.ensureScripts()
	return q
}

func (q *queue) Name() string {
	return q.name
}

func timestampInMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
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

func (q *queue) ensureScripts() {
	scripts, err := loadScripts()
	if err != nil {
		panic(err)
	}
	for key, rawScript := range scripts {
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

func (q *queue) ToKey(s string) string {
	return fmt.Sprintf("%s:%s:%s", q.queuePrefix, q.name, s)
}

func (q *queue) scriptForName(name string) (*redis.Script, bool) {
	script, ok := q.scripts[name]
	return script, ok
}

func (q *queue) Conn() redis.Conn {
	return q.pool.Get()
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

	conn := j.queue.Conn()
	jobId, err := addJobScript.Do(
		conn,
		j.queue.ToKey("id"),
		j.queue.ToKey("jobs"),
		j.queue.ToKey("waiting"),
		"", // Not supporting nullable specific Job IDs out of the box
		data,
	)

	fmt.Println("jobId: ", jobId)

	return err
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
