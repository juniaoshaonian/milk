package block_queue

import (
	"context"
	"errors"
	"sync/atomic"
	"unsafe"
)

type LinkBlockQueue[T any] struct {
	head unsafe.Pointer
	tail unsafe.Pointer
}

func (l *LinkBlockQueue[T]) Enqueue(ctx context.Context, val T) error {
	new_node := NewNode[T](val)
	newptr := unsafe.Pointer(&new_node)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tail := atomic.LoadPointer(&l.tail)
		if atomic.CompareAndSwapPointer(&l.tail, tail, newptr) {
			tailnode := (*node[T])(tail)
			atomic.StorePointer(&tailnode.next, newptr)
			return nil
		}
	}
}

func (l *LinkBlockQueue[T]) Dequeue(ctx context.Context) (T, error) {
	for {
		head := atomic.LoadPointer(&l.head)
		headnode := (*node[T])(head)
		tail := atomic.LoadPointer(&l.tail)
		tailnode := (*node[T])(tail)
		if headnode == tailnode {
			var t T
			return t, errors.New("队列为空")
		}
		headNextPtr := atomic.LoadPointer(&headnode.next)
		if atomic.CompareAndSwapPointer(&l.head, head, headNextPtr) {
			val := headnode.val
			return val, nil
		}

	}
}

type node[T any] struct {
	val  T
	next unsafe.Pointer
}

func NewNode[T any](val any) node[T] {
	return node[T]{
		val: val,
	}
}
