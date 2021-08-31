package airq

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/hashicorp/go-multierror"
)

// Queue holds a reference to a redis connection and a queue name.
type Queue struct {
	conn redis.Conn
	Name string
	Pool *redis.Pool
}

type LoopOptions struct {
	Size  int
	Sleep time.Duration
}

type Option func(*Queue)

func WithConn(c redis.Conn) Option  { return func(q *Queue) { q.conn = c } }
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
func (q *Queue) Loop(cb func([]*Job, error), opts *LoopOptions) {
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

// New defines a new Queue
func New(name string, opt Option) *Queue {
	q := &Queue{Name: name}
	opt(q)
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
	keysAndArgs := redis.Args{q.Name}
	for _, j := range jobs {
		keysAndArgs = keysAndArgs.AddFlat(j.String())
		ids = append(ids, j.ID)
	}
	ok, err := redis.Int(pushScript.Do(c, keysAndArgs...))
	if err == nil && ok != 1 {
		err = fmt.Errorf("can't add all jobs %v to queue %s", jobs, q.Name)
	}
	return ids, err
}

// Pending returns the count of jobs pending, including scheduled jobs that are not due yet.
func (q *Queue) Pending() (int64, error) {
	c, managed := q.Conn()
	if managed {
		defer c.Close()
	}
	return redis.Int64(c.Do("ZCARD", q.Name))
}

// Pop removes and returns a single job from the queue. Safe for concurrent use
// (multiple goroutines must use their own Queue objects and redis connections)
func (q *Queue) Pop() (*Job, error) {
	jobs, err := q.PopJobs(1)
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, nil
	}
	return jobs[0], nil
}

// PopJobs returns multiple jobs from the queue. Safe for concurrent use
// (multiple goroutines must use their own Queue objects and redis connections)
func (q *Queue) PopJobs(limit int) (res []*Job, err error) {
	if limit == 0 {
		return res, fmt.Errorf("limit 0")
	}
	c, managed := q.Conn()
	if managed {
		defer c.Close()
	}
	redisRes, err := redis.Strings(popJobsScript.Do(
		c, q.Name, time.Now().UnixNano(), limit,
	))
	if err != nil {
		return nil, err
	}
	var mErr error
	for _, r := range redisRes {
		j, err := newFromString(r)
		if err != nil {
			mErr = multierror.Append(mErr, err)
			continue
		}
		res = append(res, j)
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
	ok, err := redis.Int(removeScript.Do(c, redis.Args{q.Name}.AddFlat(ids)...))
	if err == nil && ok != 1 {
		err = fmt.Errorf("can't delete all jobs %v in queue %s", ids, q.Name)
	}
	return err
}
