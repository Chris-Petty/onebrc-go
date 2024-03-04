package main

import "sync"

func main() {
	wg := new(sync.WaitGroup)
	in := make(chan int, 50)

	counter := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			println("start worker", i)
			defer println("end worker", i)
			for n := range in {
				counter += n
				println("inside", i, n)
			}
		}()
	}

	for range 100 {
		in <- 1
	}

	close(in)
	wg.Wait()
	println(counter)
	// close(out)

	// close(in)
}
