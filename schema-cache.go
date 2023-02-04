package kafkalib

import (
	"time"
)

type (
	CacheWithTTL[T any] struct {
		ttl  time.Duration
		data map[string]*dataItem[T]
	}

	dataItem[T any] struct {
		data *T
		exp  time.Time
	}
)

// NewSchemaCache is a key-value cache with TTL.
// Cache is not thread-safe. Concurrency should be managed by client.
func NewSchemaCache[T any](ttl time.Duration) *CacheWithTTL[T] {
	return &CacheWithTTL[T]{
		data: make(map[string]*dataItem[T]),
		ttl:  ttl,
	}
}

func (c *CacheWithTTL[T]) Set(key string, item *T) {
	c.data[key] = &dataItem[T]{
		data: item,
		exp:  time.Now().Add(c.ttl),
	}
}

func (c *CacheWithTTL[T]) Get(key string) (*T, bool) {
	item, ok := c.data[key]

	if !ok {
		return nil, false
	}

	if time.Until(item.exp) < 0 {
		return nil, false
	}

	return item.data, true
}

func (c *CacheWithTTL[T]) Delete(key string) {
	delete(c.data, key)
}
