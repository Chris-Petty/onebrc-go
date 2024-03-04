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
//
// 3rd - out of order after much mucking around. Worked out that sending short strings in channel is super slow. []string of 1000 to pool of workers goes to 250+MBps.
// But, as it turns out what I was doing with a returning channel would create deadlock. So I used go routine with anon function and tried using closures to see what happens
//// Full file, 320MBps 41s... but the analysis is wrong and different every run LOL. Checking len(measurements) in the routines show it's a mess, ~300 lines rather than 50k
// Closures not working how I'd guess from a little side experiment with a counter. Measurements is getting mixed up between threads whoops.
//
// 4th - goroutines but feeding them with a buffered channel! Outputting by appending to an array captured in the closure appears to be safe.
//// full file: 45.5s, ~300MBps. Correct output! Curiously measured the aggregation of analysis at the end, only 0.5s. Not worth improving!
// chunkSize 50k seems to be a sweet spot for ~300MBps. 5k: ~150MBps. 500k+: ~280MBps
//
//

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CityAnalysis struct {
	min   float64
	max   float64
	sum   float64
	count uint64
}

func (ca CityAnalysis) toString() string {
	return fmt.Sprintf("%v/%v/%v", math.Round(ca.min*10)/10, math.Round((ca.sum/float64(ca.count))*10)/10, math.Round(ca.max*10)/10)
}

func main() {

	file, err := os.Open("../1brc/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	chunkSize := 500000
	chunkBuffer := make(chan []string, 10)
	wg := new(sync.WaitGroup)
	analysisChunks := make([]map[string]CityAnalysis, 0)
	measurements := make([]string, 0)

	for {
		b := scanner.Scan()
		if b {
			measurements = append(measurements, scanner.Text())
		}
		if len(measurements) >= chunkSize || !b {
			chunkBuffer <- measurements
			measurements = make([]string, 0)
			wg.Add(1)
			go func() {
				chunk := <-chunkBuffer
				// id := rand.Intn(1000000)
				// println("routine", id, len(chunk))
				defer wg.Done()
				analysisChunk := make(map[string]CityAnalysis)
				for _, line := range chunk {
					parts := strings.Split(line, ";") // 180MBps here
					cityName := parts[0]
					cityTemp, _ := strconv.ParseFloat(parts[1], 32)

					cityAnalysis := analysisChunk[cityName]
					cityAnalysis.count++
					cityAnalysis.sum += cityTemp
					if cityTemp > cityAnalysis.max {
						cityAnalysis.max = cityTemp
					}
					if cityTemp < cityAnalysis.min {
						cityAnalysis.min = cityTemp
					}
					analysisChunk[cityName] = cityAnalysis
				}
				analysisChunks = append(analysisChunks, analysisChunk)
			}()
			if !b {
				break
			}
		}
	}

	println("closing...")
	println("waiting...")
	wg.Wait()
	println("continuing...")
	println(len(analysisChunks))

	// Aggregate partial analyses
	s := time.Now()
	analysis := make(map[string]CityAnalysis)
	for _, ca := range analysisChunks {
		for k, v := range ca {
			cityFinal := analysis[k]
			cityFinal.count += v.count
			cityFinal.sum += v.sum
			if cityFinal.min > v.min {
				cityFinal.min = v.min
			}
			if cityFinal.max < v.max {
				cityFinal.max = v.max
			}
			analysis[k] = cityFinal
		}
	}

	var output []string
	for cityName, cityAnalysis := range analysis {
		output = append(output, fmt.Sprintf("%v=%v", cityName, cityAnalysis.toString()))
	}
	slices.Sort(output)
	strings.Join(output, ", ")
	fmt.Println(output)

	e := time.Now()
	fmt.Printf("\n%v\n", e.Sub(s))
}
