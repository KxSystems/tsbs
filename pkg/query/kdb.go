package query

import (
	"fmt"
	"log"
	"sync"
)

// type KDB_Query struct {
// 	Api  string
// 	Args []byte
// }

type KDB struct {
	HumanLabel       []byte
	HumanDescription []byte
	Query            []string
	id               uint64
}

// KDBPool is a sync.Pool of KDB Query types
var KDBPool = sync.Pool{
	New: func() interface{} {
		log.Print("Getting new KDB.Query")
		return &KDB{
			HumanLabel:       make([]byte, 0),
			HumanDescription: make([]byte, 0),
			Query:            make([]string, 0),
		}
	},
}

// NewKDB returns a new KDB type Query
func NewKDB() *KDB {
	log.Print("Getting new KDB Query")
	return KDBPool.Get().(*KDB)
}

// GetID returns the ID of this Query
func (q *KDB) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *KDB) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *KDB) String() string {
	return fmt.Sprintf("HumanLabel: \"%s\", HumanDescription: \"%s\", Query: \"%s\"", q.HumanLabel, q.HumanDescription, q.Query)
}

// HumanLabelName returns the human readable name of this Query
func (q *KDB) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *KDB) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *KDB) Release() {
	log.Print("Releasing values for KDB query")
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.Query = q.Query[:0]
	q.id = 0

	log.Print("Released")
	KDBPool.Put(q)
}
