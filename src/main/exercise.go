package main

import (
	"sync"
	
)

// import "time"

func main() {
	counter := 0
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	for i := 0; i < 1000; i++ {
		go func() {
			mu.Lock() 
			counter = counter + 1
			cond.Broadcast()
			mu.Unlock()
		}()
	}
	mu.Lock()
	for counter < 1000 {
		cond.Wait()
  	}
	if counter >= 1000 {
		println(counter)
	}
	mu.Unlock()
}
