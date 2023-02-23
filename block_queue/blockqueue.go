package block_queue

import (
	"context"
	"sync"
)

type BlockQueue[T any] struct {
	data      []T
	maxSize   int
	start     int
	end       int
	number    int
	mu        *sync.RWMutex
	emptyCond *Cond
	fullCond  *Cond
}

func (b *BlockQueue[T]) Enqueue(ctx context.Context, val T) error {
	b.mu.Lock()
	for b.number == b.maxSize {
		err := b.fullCond.WaitTimeOut(ctx)
		if err != nil {
			return err
		}
	}
	b.end = (b.end + 1) % b.maxSize
	b.data[b.end] = val
	b.number++
	b.emptyCond.Signal()
	b.mu.Unlock()
	return nil
}

func (b *BlockQueue[T]) Dequeue(ctx context.Context) (T, error) {
	b.mu.Lock()
	for b.number == 0 {
		err := b.emptyCond.WaitTimeOut(ctx)
		if err != nil {
			var t T
			return t, err
		}
	}
	val := b.data[b.start]
	b.start = (b.start + 1) % b.maxSize
	b.number--
	b.fullCond.Signal()
	b.mu.Unlock()
	return val, nil
}

type Cond struct {
	*sync.Cond
}

func NewCond(l sync.Locker) *Cond {
	c := sync.NewCond(l)
	return &Cond{
		Cond: c,
	}
}

func NewQueue[T any](max int) *BlockQueue[T] {
	mu := &sync.RWMutex{}
	emptyCond := NewCond(mu)
	fullCond := NewCond(mu)
	data := make([]T, max)
	return &BlockQueue[T]{
		data:      data,
		maxSize:   max,
		emptyCond: emptyCond,
		fullCond:  fullCond,
		mu:        mu,
	}

}

func (c *Cond) WaitTimeOut(ctx context.Context) error {
	ch := make(chan struct{})
	go func() {
		c.Wait()
		select {
		case ch <- struct{}{}:
		default:
			c.Signal()
			c.L.Unlock()
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}
