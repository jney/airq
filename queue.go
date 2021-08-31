package airq

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Queue holds a reference to a redis connection and a queue name.
type Queue struct {
	conn   redis.Conn
	name   string
	Pool   *redis.Pool
	prefix string

	// scripts
	popJobsScript *redis.Script
	pushScript    *redis.Script
	removeScript  *redis.Script
}

type LoopOptions struct {
	Size  int
	Sleep time.Duration
}

type Option func(*Queue)

func WithConn(c redis.Conn) Option  { return func(q *Queue) { q.conn = c } }
func WithPrefix(p string) Option    { return func(q *Queue) { q.prefix = p } }
func WithPool(p *redis.Pool) Option { return func(q *Queue) { q.Pool = p } }

func (q *Queue) Conn() (redis.Conn, bool) {
	if q.conn == nil && q.Pool == nil {
		panic("no connection defined")
	}
	if q.Pool != nil {
		return q.Pool.Get(), true
	}
	return q.conn, false
}

// Loop over the queue
func (q *Queue) Loop(cb func([]string, error), opts *LoopOptions) {
	if opts == nil {
		opts = new(LoopOptions)
	}
	if opts.Size == 0 {
		opts.Size = 100
	}
	if opts.Sleep == 0 {
		opts.Sleep = 3 * time.Second
	}
	for {
		jobs, err := q.PopJobs(opts.Size)
		if err != nil || len(jobs) > 0 {
			cb(jobs, err)
			continue
		}
		time.Sleep(opts.Sleep)
	}
}

func (q *Queue) Name() string { return q.prefix + q.name }

// New defines a new Queue
func New(name string, opt Option) *Queue {
	q := &Queue{name: name, prefix: "airq:"}
	opt(q)
	q.popJobsScript = redis.NewScript(1, fmt.Sprintf(popJobsScript, q.prefix))
	q.pushScript = redis.NewScript(1, fmt.Sprintf(pushScript, q.prefix))
	q.removeScript = redis.NewScript(1, fmt.Sprintf(removeScript, q.prefix))
	return q
}

// Push schedule a job at some point in the future, or some point in the past.
// Scheduling a job far in the past is the same as giving it a high priority,
// as jobs are popped in order of due date.
func (q *Queue) Push(jobs ...*Job) (ids []string, err error) {
	if len(jobs) == 0 {
		return ids, fmt.Errorf("no jobs provided")
	}
	c, managed := q.Conn()
	if managed {
		defer c.Close()
	}
	keysAndArgs := redis.Args{q.name}
	for _, j := range jobs {
		keysAndArgs = keysAndArgs.AddFlat(j.String())
		ids = append(ids, j.ID)
	}
	ok, err := redis.Int(q.pushScript.Do(c, keysAndArgs...))
	if err == nil && ok != 1 {
		err = fmt.Errorf("can't add all jobs %v to queue %s", jobs, q.name)
	}
	return ids, err
}

// Pending returns the count of jobs pending, including scheduled jobs that are not due yet.
func (q *Queue) Pending() (int64, error) {
	c, managed := q.Conn()
	if managed {
		defer c.Close()
	}
	return redis.Int64(c.Do("ZCARD", q.prefix+q.name))
}

// Pop removes and returns a single job from the queue. Safe for concurrent use
// (multiple goroutines must use their own Queue objects and redis connections)
func (q *Queue) Pop() (string, error) {
	jobs, err := q.PopJobs(1)
	if err != nil {
		return "", err
	}
	if len(jobs) == 0 {
		return "", nil
	}
	return jobs[0], nil
}

// PopJobs returns multiple jobs from the queue. Safe for concurrent use
// (multiple goroutines must use their own Queue objects and redis connections)
func (q *Queue) PopJobs(limit int) (res []string, err error) {
	if limit == 0 {
		return []string{}, fmt.Errorf("limit 0")
	}
	c, managed := q.Conn()
	if managed {
		defer c.Close()
	}
	redisRes, err := redis.Strings(q.popJobsScript.Do(
		c, q.name, time.Now().UnixNano(), limit,
	))
	if err != nil {
		return nil, err
	}
	for _, r := range redisRes {
		res = append(res, uncompress(r))
	}
	return res, nil
}

// Remove removes a job from the queue
func (q *Queue) Remove(ids ...string) error {
	if len(ids) == 0 {
		return fmt.Errorf("no id provided")
	}
	c, managed := q.Conn()
	if managed {
		defer c.Close()
	}
	ok, err := redis.Int(q.removeScript.Do(c, redis.Args{q.name}.AddFlat(ids)...))
	if err == nil && ok != 1 {
		err = fmt.Errorf("can't delete all jobs %v in queue %s", ids, q.name)
	}
	return err
}
