package airq

import (
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func setup(t *testing.T) (*Queue, func()) {
	t.Parallel()
	name := randomName()
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	q := New(name, WithConn(c))
	teardown := func() {
		conn, managed := q.Conn()
		if managed {
			defer conn.Close()
		}
		conn.Send("DEL", q.Name)
		conn.Send("DEL", q.Name+":values")
		conn.Close()
	}
	return q, teardown
}

func addJobs(t *testing.T, q *Queue, jobs ...Job) {
	for _, job := range jobs {
		if _, err := q.Push(&job); err != nil {
			t.Error(err)
			t.FailNow()
		}
	}
}

func randomName() string {
	b := make([]byte, 12)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

func TestQueueTasks(t *testing.T) {
	q, teardown := setup(t)
	defer teardown()

	_, err := q.Push(&Job{Content: "basic item 1"})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	ids, err := q.Push(&Job{Content: "basic item 1"})
	if err != nil {
		t.Error(err, ids)
		t.FailNow()
	}

	pending, _ := q.Pending()
	if pending != 1 {
		t.Error("Expected 1 job pending in queue, was", pending)
		t.FailNow()
	}

	// it adds a `Unique` job
	_, err = q.Push(&Job{Content: "basic item 1", Strategy: CreateStrategy})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	pending, _ = q.Pending()
	if pending != 2 {
		t.Error("Expected 2 jobs pending in queue, was", pending)
		t.FailNow()
	}

	// it adds 2 jobs at once
	_, err = q.Push(&Job{Content: "basic item 2"}, &Job{Content: "basic item 3"})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	pending, _ = q.Pending()
	if pending != 4 {
		t.Error("Expected 4 jobs pending in queue, was", pending)
	}
}

func TestQueueTaskScheduling(t *testing.T) {
	q, teardown := setup(t)
	defer teardown()

	_, err := q.Push(&Job{Content: "scheduled item 1", When: time.Now().Add(90 * time.Millisecond)})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	pending, err := q.Pending()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if pending != 1 {
		t.Error("Expected 1 job pending in queue, was", pending)
	}

	job, err := q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job != nil {
		t.Error("Didn't expect to get a job off the queue but I got one.")
	}

	// Wait for the job to become ready.
	time.Sleep(100 * time.Millisecond)

	job, err = q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job.Content != "scheduled item 1" {
		t.Error("Expected to get a job off the queue, but I got this:", job)
	}
}

func TestPopOrder(t *testing.T) {
	q, teardown := setup(t)
	defer teardown()

	addJobs(t, q,
		Job{Content: "oldest", When: time.Now().Add(-300 * time.Millisecond)},
		Job{Content: "newer", When: time.Now().Add(-100 * time.Millisecond)},
		Job{Content: "older", When: time.Now().Add(-200 * time.Millisecond)},
	)

	job, err := q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job.Content != "oldest" {
		t.Error("Expected to the oldest job off the queue, but I got this:", job)
	}

	job, err = q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job.Content != "older" {
		t.Error("Expected to the older job off the queue, but I got this:", job)
	}

	job, err = q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job.Content != "newer" {
		t.Error("Expected to the newer job off the queue, but I got this:", job)
	}

	job, err = q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if job != nil {
		t.Error("Expected no jobs")
	}
}

func TestPopMultiOrder(t *testing.T) {
	q, teardown := setup(t)
	defer teardown()

	addJobs(t, q,
		Job{Content: "oldest", When: time.Now().Add(-300 * time.Millisecond)},
		Job{Content: "newer", When: time.Now().Add(-100 * time.Millisecond)},
		Job{Content: "older", When: time.Now().Add(-200 * time.Millisecond)},
	)

	jobs, err := q.PopJobs(3)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	for i, expected := range []string{"oldest", "older", "newer"} {
		if jobs[i].Content != expected {
			t.Error("Expected to having jobs off the queue:", expected, " but I got this:", jobs)
		}
	}

	job, err := q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if job != nil {
		t.Error("Expected no jobs")
	}
}

func TestRemove(t *testing.T) {
	q, teardown := setup(t)
	defer teardown()

	addJobs(t, q,
		Job{Content: "oldest", When: time.Now().Add(-300 * time.Millisecond), ID: "01"},
		Job{Content: "newer", When: time.Now().Add(-100 * time.Millisecond)},
		Job{Content: "older", When: time.Now().Add(-200 * time.Millisecond), ID: "02"},
	)

	q.Remove("01", "02")

	jobs, err := q.PopJobs(3)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	expected := "newer"
	if jobs[0].Content != expected {
		t.Error("Expected to having jobs off the queue:", expected, " but I got this:", jobs)
	}
}
