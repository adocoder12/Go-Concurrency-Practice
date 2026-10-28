package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"time"
)

const (
	numOrders  = 20
	numWorkers = 3
)

var ErrOutOfStock = errors.New("out of stock")

type Order struct {
	ID   int
	Item string
}

// Result carries the order and, if it failed, the error
type Result struct {
	Worker int
	Order  Order
	Err    error
}

type Summary struct {
	Succeeded  int
	OutOfStock int
	Failed     int
}

func main() {
	// Ctrl+C cancels ctx, and everything stops cleanly
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	orders := produce(ctx, generateOrders(numOrders, []string{"Pizza", "Burger"}))
	results := startWorkers(ctx, numWorkers, orders)

	// Blocks until all workers are done and results is closed
	summary := handleResults(results)

	if ctx.Err() != nil {
		fmt.Println("-- Stopped early ---")
	} else {
		fmt.Println("-- All orders processed ---")
	}
	fmt.Printf("Succeeded: %d, Out of stock: %d, Other failures: %d\n",
		summary.Succeeded, summary.OutOfStock, summary.Failed)
}

// produce sends orders on a channel it owns, and closes it when done or cancelled
func produce(ctx context.Context, orders []Order) <-chan Order {
	out := make(chan Order)
	go func() {
		defer close(out)
		for _, order := range orders {
			select {
			case out <- order:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// startWorkers runs n workers and closes results once all of them are done
func startWorkers(ctx context.Context, n int, orders <-chan Order) <-chan Result {
	results := make(chan Result)
	var wg sync.WaitGroup

	for w := 1; w <= n; w++ {
		wg.Go(func() {
			worker(ctx, w, orders, results)
		})
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

// worker takes orders until the channel is closed or ctx is cancelled
func worker(ctx context.Context, id int, orders <-chan Order, results chan<- Result) {
	for {
		select {
		case <-ctx.Done():
			return
		case order, ok := <-orders:
			if !ok {
				return
			}
			err := processOrder(ctx, order)
			if ctx.Err() != nil {
				return // cancelled mid-order: don't report it
			}
			results <- Result{Worker: id, Order: order, Err: err}
		}
	}
}

// processOrder simulates the actual work for one order
func processOrder(ctx context.Context, order Order) error {
	select {
	case <-time.After(time.Duration(rand.IntN(300)) * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	switch rand.IntN(10) {
	case 0:
		// %w wraps the sentinel so errors.Is can find it
		return fmt.Errorf("order %d %s: %w", order.ID, order.Item, ErrOutOfStock)
	case 1:
		return fmt.Errorf("order %d: payment declined", order.ID)
	}
	return nil
}

// handleResults prints every result and counts each outcome
func handleResults(results <-chan Result) Summary {
	var s Summary
	for r := range results {
		if r.Err != nil {
			if errors.Is(r.Err, ErrOutOfStock) {
				fmt.Printf("Worker %d OUT OF STOCK: %v\n", r.Worker, r.Err)
				s.OutOfStock++
				continue
			}
			fmt.Printf("Worker %d FAILED: %v\n", r.Worker, r.Err)
			s.Failed++
			continue
		}
		fmt.Printf("Worker %d processed order #%d (%s)\n", r.Worker, r.Order.ID, r.Order.Item)
		s.Succeeded++
	}
	return s
}

func generateOrders(n int, items []string) []Order {
	orders := make([]Order, n)
	for i := range n {
		orders[i] = Order{
			ID:   i + 1,
			Item: items[rand.IntN(len(items))],
		}
	}
	return orders
}