// Solving the 1brc in go
package main

// After solving 1brc with multiple processes in the 4D Language only resulted in a best time of 5:07, we're trying go!
//
// 1st - Learning go syntax, basic read file and analysis in one process/thread/whatever we call it.
//// full file: 2:12! ~100MBps. Too easy LOL.
//

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"slices"
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

func main() {
	s := time.Now()

	file, err := os.Open("../1brc/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	aggregation := make(map[string]CityAnalysis)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")
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

	var out []string
	for cityName, cityAnalysis := range aggregation {
		out = append(out, fmt.Sprintf("%v=%v", cityName, cityAnalysis.ToString()))
	}

	slices.Sort(out)
	strings.Join(out, ", ")
	fmt.Println(out)

	e := time.Now()
	fmt.Printf("\n%v\n", e.Sub(s))
}
