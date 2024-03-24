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
// 5th - I got my rust implementation to ~19s at ~750MBps. 10 threads, each with their own reader. Not to be outdone...
//// Full file 13.3s. ~1040MBps. 100% CPU usage at last.
// Here gone a little different while experimenting. Learned that the correct way to pass things into go routines is as parameters, DUH.
// So the channels I had used in earlier solutions were not the cleanest way to do things.
// Rather than each thread having a reader, I'm just reading large byte arrays into memory in main thread and passing to each goroutine.
// Initially went with as many chunks as CPUs, but it looked like I promptly ran out of memory and kernel was paging to disk.
// So I kludged it to 1000x the number of CPUs. lol.
// Using copilot at the mo, has been useful hashing out some ideas while I'm weak at go syntax.

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const PATH string = "../1brc/measurements.txt"

type CityAnalysis struct {
	min   float64
	max   float64
	sum   float64
	count uint64
}

func (ca CityAnalysis) update(cityTemp float64) CityAnalysis {
	ca.count++
	ca.sum += cityTemp
	if cityTemp > ca.max {
		ca.max = cityTemp
	}
	if cityTemp < ca.min {
		ca.min = cityTemp
	}
	return ca
}

func (ca CityAnalysis) toString() string {
	return fmt.Sprintf("%v/%v/%v", math.Round(ca.min*10)/10, math.Round((ca.sum/float64(ca.count))*10)/10, math.Round(ca.max*10)/10)
}

func position(bytes []byte, b byte) int64 {
	for i, c := range bytes {
		if c == b {
			return int64(i)
		}
	}
	return -1
}

func main() {
	s := time.Now()
	numberOfCPUs := runtime.NumCPU() * 1000 // kludge to not read such big chunks so fast that we start paging to disk... is my guess what happened
	fileInfo, _ := os.Stat(PATH)
	fileSize := fileInfo.Size()
	chunkSize := fileSize / int64(numberOfCPUs)
	analysisChunks := make([]map[string]CityAnalysis, 0)
	wg := new(sync.WaitGroup)

	file, _ := os.Open(PATH)
	defer file.Close()

	startByte := int64(0)
	for range numberOfCPUs {
		endByte := startByte + chunkSize
		if endByte > fileSize {
			endByte = fileSize
		}

		// Read chunk
		file.Seek(endByte, 0)
		// scan to end of next line
		readAheadBytes := make([]byte, 256)
		file.Read(readAheadBytes)
		newlinePos := position(readAheadBytes, '\n')
		endByte = endByte + int64(newlinePos)
		file.Seek(endByte, 0)
		chunk := make([]byte, endByte-startByte)
		file.Read(chunk)

		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()

			semicolonPos := 0
			newlinePos := 0
			cityAnalysisMap := make(map[string]CityAnalysis)
			for i, c := range chunk {
				if c == ';' {
					semicolonPos = i
				} else if c == '\n' && i > semicolonPos {
					cityName := string(chunk[newlinePos+1 : semicolonPos])
					newlinePos = i
					cityTemp, _ := strconv.ParseFloat(string(chunk[semicolonPos+1:newlinePos]), 64)
					analysis := cityAnalysisMap[cityName]
					cityAnalysisMap[cityName] = analysis.update(cityTemp)
				}
			}
			analysisChunks = append(analysisChunks, cityAnalysisMap)
		}(chunk)

		startByte = endByte
	}

	println("waiting...")
	wg.Wait()
	println("continuing...")
	println(len(analysisChunks))

	// Aggregate partial analyses
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
