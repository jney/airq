# Air Q (*r*edis *q*ueue) <a href="https://travis-ci.com/jney/airq"><img src="https://travis-ci.com/jney/airq.svg?branch=main" alt="Build Status"></a>

## What and why

This is a redis-based queue for usage in Go.
This project is a fork from [go-redis-queue](https://github.com/agilebits/go-redis-queue)
I liked this solution a lot but i missed a few features.

## Original Features

- Ability to add arbitrary tasks to a queue in redis
- Option to Dedup tasks based on the task signature.
- Ability to schedule tasks in the future.
- Atomic Push and Pop from queue. Two workers cannot get the same job.
- Sorted FIFO queue.
- Can act like a priority queue by scheduling a job with a really old timestamp
- Simple API

## Added Features

- Insert multiple job at once
- Remove a job
- Have multiple times the same job (same content)

## Usage

Adding jobs to a queue.

```go
import "github.com/jney/airq"
```

```go
c, err := redis.Dial("tcp", "127.0.0.1:6379")
if err != nil { ... }
defer c.Close()

q := airq.New("queue_name", airq.WithConn(c))

added, taskID, err := q.Push(&airq.Job{Content: "basic item"})
if err != nil { ... }

queueSize, err := q.Pending()
if err != nil { ... }

added, taskID, err := q.Push(&airq.Job{
  Content: "scheduled item",
  When: time.Now().Add(10*time.Minute),
})
if err != nil { ... }
```

A simple worker processing jobs from a queue:

```go
c, err := redis.Dial("tcp", "127.0.0.1:6379")
if err != nil { ... }
defer c.Close()

q := airq.New("queue_name", airq.WithConn(c))

for !timeToQuit {
  job, err = q.Pop()
  if err != nil { ... }
  if job != nil {
    // process the job.
  } else {
    time.Sleep(2*time.Second)
  }
}
```

A batch worker processing jobs from a queue:

```go
c, err := redis.Dial("tcp", "127.0.0.1:6379")
if err != nil { ... }
defer c.Close()

q := airq.New("queue_name", airq.WithConn(c))

q.Loop(func (jobs []*airq.Job, err error) {
  for i, job := range jobs {
    // process the job.
  }
}, nil)
```

## TODO

- pass context
- check if working with `[]byte` can be done
