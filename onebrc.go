// Solving the 1brc in go
package main

// After solving 1brc with multiple processes in the 4D Language only resulted in a best time of 5:07, we're trying go!
//
// 1st - Learning go syntax, basic read file and analysis in one process/thread/whatever we call it.
//// full file: 2:12! ~100MBps. Too easy LOL.
//
// 2nd - learning concurrency in go! Hasn't gone well, sending one line at a time to a pool of workers is much slower!
// Tried with one channel shared by 10 workers and a channel for each worker and sending lines in a round robin, not much difference.
//// full file (estimate) ~3:10, ~70MBps. Clearing not doing things right.

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type CityAnalysis struct {
	min   float64
	max   float64
	sum   float64
	count uint64
}

func (ca CityAnalysis) ToString() string {
	return fmt.Sprintf("%v/%v/%v", math.Round(ca.min*10)/10, math.Round((ca.sum/float64(ca.count))*10)/10, math.Round(ca.max*10)/10)
}

func lineProcessor(in chan string, out chan map[string]CityAnalysis) {
	aggregation := make(map[string]CityAnalysis)

	for line := range in {
		parts := strings.Split(line, ";") // 180MBps here

		cityName := parts[0]
		cityTemp, _ := strconv.ParseFloat(parts[1], 32)

		analysis := aggregation[cityName]

		analysis.count++
		analysis.sum += cityTemp
		if cityTemp > analysis.max {
			analysis.max = cityTemp
		}
		if cityTemp < analysis.min {
			analysis.min = cityTemp
		}

		aggregation[cityName] = analysis
	}

	out <- aggregation
	fmt.Printf("stop worker%v\n")
}

type Worker struct {
	in  chan string
	out chan map[string]CityAnalysis
}

func makeWorker() Worker {
	in := make(chan string, 100)
	out := make(chan map[string]CityAnalysis)
	w := Worker{in, out}
	fmt.Println("start worker")
	go lineProcessor(in, out)
	return w
}

func main() {
	s := time.Now()

	file, err := os.Open("../1brc/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	workerCount := 10
	workerPool := make([]Worker, workerCount)
	for i := range workerPool {
		workerPool[i] = makeWorker()
	}

	// Probably a much more elegant/idiomatic way of doing this lol
	i := 0
	for scanner.Scan() {
		workerPool[i].in <- scanner.Text()
		i++
		if i >= workerCount {
			i = 0
		}
	}

	var aggs []map[string]CityAnalysis
	for i := range workerPool {
		close(workerPool[i].in)
		aggs = append(aggs, <-workerPool[i].out)
	}

	//TODO aggregate

	// var output []string
	// for cityName, cityAnalysis := range aggregation {
	// 	output = append(output, fmt.Sprintf("%v=%v", cityName, cityAnalysis.ToString()))
	// }

	// slices.Sort(output)
	// strings.Join(output, ", ")
	// fmt.Println(output)

	e := time.Now()
	fmt.Printf("\n%v\n", e.Sub(s))
}
