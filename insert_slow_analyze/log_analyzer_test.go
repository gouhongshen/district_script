package insert_slow_analyze

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

/*

|----------------------------|
	|--|  |---|  |--|
		|-----------------|
*/

func lineExtractor(lineStream chan string) {
	for {
		select {
		case line, ok := <-lineStream:
			if !ok {
				return
			}

			idx := strings.Index(line, "{")
			if idx == -1 {
				continue
			}

			line = line[idx:]
			mm := map[string]string{}

			if err := json.Unmarshal([]byte(line), &mm); err != nil {
				fmt.Println("decode line err")
				continue
			}

			fmt.Println(mm)
		}
	}
}

func Test_Main(t *testing.T) {
	file, err := os.Open("slow_log.txt")
	if err != nil {
		fmt.Println("open slow_log.txt failed", err.Error())
	}

	scanner := bufio.NewScanner(file)

	const maxCapacity int = 1024 * 1024 * 100
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	lineStream := make(chan string)

	go lineExtractor(lineStream)

	for scanner.Scan() {
		lineStream <- scanner.Text()
	}

}
