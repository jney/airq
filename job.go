package airq

import (
	"bytes"
	"compress/gzip"
	"hash/maphash"
	"io/ioutil"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/shamaton/msgpackgen/msgpack"
)

// Job is the struct of job in queue
type Job struct {
	CompressedContent string    `msgpack:"content"`
	Content           string    `msgpack:"-"`
	ID                uint64    `msgpack:"id"`
	Unique            bool      `msgpack:"-"`
	When              time.Time `msgpack:"-"`
	WhenUnixNano      int64     `msgpack:"when"`
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

func (j *Job) generateID() uint64 {
	if j.Unique {
		return new(maphash.Hash).Sum64()
	}
	return xxhash.Sum64String(j.Content)
}

func (j *Job) setDefaults() {
	j.CompressedContent = compress(j.Content)
	if j.When.IsZero() {
		j.When = time.Now()
	}
	j.WhenUnixNano = j.When.UnixNano()
	if j.ID == 0 {
		j.ID = j.generateID()
	}
}

func (j *Job) String() string {
	j.setDefaults()
	b, _ := msgpack.Marshal(j)
	return string(b)
}
