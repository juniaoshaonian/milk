package block_queue

import (
	"context"
	"sync"
)

type BlockQueueV1[T any] struct {
	data      []T
	maxSize   int
	start     int
	end       int
	number    int
	mu        *sync.RWMutex
	emptyCond *CondV1
	fullCond  *CondV1
}

func (b *BlockQueueV1[T]) Enqueue(ctx context.Context, val T) error {
	b.mu.Lock()
	for b.number == b.maxSize {
		err := b.fullCond.WaitTimeout(ctx)
		if err != nil {
			b.mu.Unlock()
			return err
		}
	}
	b.end = (b.end + 1) % b.maxSize
	b.data[b.end] = val
	b.number++
	b.emptyCond.Broadcast()
	return nil
}

func (b *BlockQueueV1[T]) Dequeue(ctx context.Context) (T, error) {
	b.mu.Lock()
	for b.number == b.maxSize {
		err := b.emptyCond.WaitTimeout(ctx)
		if err != nil {
			b.mu.Unlock()
			var t T
			return t, err
		}
	}
	val := b.data[b.start]
	b.start = (b.start + 1) % b.maxSize
	b.emptyCond.Broadcast()
	b.number--
	return val, nil
}

type CondV1 struct {
	l  sync.Locker
	ch chan struct{}
}

func NewCondV1(l sync.Locker) *CondV1 {
	c := &CondV1{
		l:  l,
		ch: make(chan struct{}),
	}
	return c
}

func (c *CondV1) WaitTimeout(ctx context.Context) error {
	c.l.Unlock()
	ch := c.ch
	select {
	case <-ctx.Done():
		c.l.Lock()
		return ctx.Err()
	case <-ch:
		c.l.Lock()
		return nil
	}
}

func (c *CondV1) Broadcast() {
	ch := make(chan struct{})
	old := c.ch
	c.ch = ch
	c.l.Unlock()
	close(old)
}
