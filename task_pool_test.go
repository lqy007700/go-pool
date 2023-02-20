package go_pool

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestA(t *testing.T) {
	pool, err := NewOnDemandBlockTaskPool(10, 10)
	if err != nil {
		return
	}

	_ = pool.Start()

	pool.Submit(context.Background(), TaskFunc(func(ctx context.Context) error {
		fmt.Println(123)
		return nil
	}))

	pool.Submit(context.Background(), TaskFunc(func(ctx context.Context) error {
		time.Sleep(time.Second * 5)
		fmt.Println(456)
		return nil
	}))

}

func task1(ctx context.Context) error {

	fmt.Println("123123")
	return nil
}
