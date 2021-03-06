package airq

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/rs/xid"
	"github.com/shamaton/msgpackgen/msgpack"
)

type Strategy int

const (
	UpdateStrategy Strategy = iota // update the job with same signature (change execution time)
	CreateStrategy                 // create a new job even if it's the same signature
	KeepStrategy                   // [TODO] keep the one already in keep (don't do anything)
)

// Job is the struct of job in queue
type Job struct {
	CompressedContent string    `msgpack:"content"`
	Content           string    `msgpack:"-"`
	ID                string    `msgpack:"id"`
	Strategy          Strategy  `msgpack:"-"`
	Subject           string    `msgpack:"subject"`
	When              time.Time `msgpack:"-"`
	WhenUnixNano      int64     `msgpack:"when"`
}

func newJobFromString(in string) (*Job, error) {
	var j Job
	if err := msgpack.Unmarshal([]byte(in), &j); err != nil {
		return nil, err
	}
	j.Content = uncompress(j.CompressedContent)
	return &j, nil
}

func compress(in string) string {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	gz.Write([]byte(in))
	gz.Flush()
	gz.Close()
	return b.String()
}

func uncompress(in string) string {
	rdata := bytes.NewReader([]byte(in))
	r, _ := gzip.NewReader(rdata)
	s, _ := ioutil.ReadAll(r)
	return string(s)
}

func (j *Job) generateID() string {
	if j.Strategy == CreateStrategy {
		return xid.New().String()
	}
	return strconv.FormatUint(xxhash.Sum64String(j.Content), 10)
}

func (j *Job) setDefaults() {
	j.CompressedContent = compress(j.Content)
	if j.When.IsZero() {
		j.When = time.Now()
	}
	j.WhenUnixNano = j.When.UnixNano()
	if j.ID == "" {
		j.ID = j.generateID()
	}
}

func (j *Job) String() string {
	j.setDefaults()
	b, _ := msgpack.Marshal(j)
	return string(b)
}
