package threadpool

import (
	"fmt"
	"testing"
	"time"

	. "github.com/AlexandreChamard/go-functor"
)

func TestThreadPool(t *testing.T) {
	tp := NewThreadPool(ThreadPoolConfig{
		PoolSize:  100,
		EnableLog: true,
	})

	f := func(i int) {
		time.Sleep(100 * time.Millisecond)
		// fmt.Println("coucou", i)
	}

	for n := 0; n < 100; n++ {
		tp.SubmitPriority(MakeFunctor1_0(f, n), n/10)
	}

	tp.Stop()
	tp.Wait()
}

func BenchmarkThreadPool(b *testing.B) {
	b.Log("Start benchmark")

	tp := NewThreadPool(ThreadPoolConfig{
		PoolSize:  1,
		EnableLog: true,
	})

	f := func(i int) { fmt.Println("coucou", i) }

	for n := 0; n < 100; n++ {
		tp.SubmitPriority(MakeFunctor1_0(f, n), n/10)
	}

	// time.Sleep(1 * time.Second)

	tp.Stop()
	tp.Wait()
}
