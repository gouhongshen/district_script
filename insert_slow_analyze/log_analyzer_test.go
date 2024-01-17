package insert_slow_analyze

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type Item struct {
	phaseMap                           map[string][][2]int64
	rngHit, rngTotal, rngStart, rngEnd int64
	txnStart, txnEnd                   int64
	cnCommitStart, cnCommitEnd         int64
}

var itemPool = sync.Pool{
	New: func() any {
		return newItem()
	},
}

func newItem() *Item {
	item := new(Item)
	item.phaseMap = make(map[string][][2]int64)
	return item
}

// input (1705481901982721474,1705481901984339420)
func decodePair(str string) (start, end int64, err error) {
	if len(str) == 0 {
		return
	}

	if str[0] != '(' || str[len(str)-1] != ')' {
		fmt.Println("pair input err")
		return 0, 0, errors.New("")
	}

	str = str[1:]
	str = str[:len(str)-1]
	pair := strings.Split(str, ",")

	start, err = strconv.ParseInt(pair[0], 10, 64)
	if err != nil {
		fmt.Println("decode pair failed", err.Error())
		return
	}

	end, err = strconv.ParseInt(pair[1], 10, 64)
	if err != nil {
		fmt.Println("decode pair failed", err.Error())
		return
	}

	return
}

// sql output:(1705481901982721474,1705481901984339420)(1705481901984339974,1705481902126540127);projection:(1705481947931432312,1705481947931912779)(1705481947931432206,1705481947931973741)
func extractPhase(mm map[string]string, item *Item) bool {
	phase, ok := mm["phase duration"]
	if !ok {
		fmt.Println("no phase duration")
		return false
	}

	durs := strings.Split(phase, ";")
	for idx := range durs {
		if len(durs[idx]) == 0 {
			continue
		}

		x := strings.Index(durs[idx], ":")
		if x == -1 {
			return false
		}
		pName := durs[idx][:x]
		pairs := durs[idx][x+1:]

		for len(pairs) > 1 {
			x = strings.Index(pairs, ")")
			if x == -1 {
				return false
			}

			start, end, err := decodePair(pairs[0 : x+1])
			if err != nil {
				return false
			}

			item.phaseMap[pName] = append(item.phaseMap[pName], [2]int64{start, end})
			pairs = pairs[x+1:]
		}
	}
	return true
}

// "txnLifeSpan":"5ms(1705481947927334447,1705481947932637419)"
func extractTxnLifeSpan(mm map[string]string, item *Item) bool {
	lifeStr, ok := mm["txnLifeSpan"]
	if !ok {
		fmt.Println("no txnLifeSpan")
		return false
	}

	x := strings.Index(lifeStr, "(")
	if x == -1 {
		return false
	}

	start, end, err := decodePair(lifeStr[x:])
	if err != nil {
		return false
	}

	item.txnStart = start
	item.txnEnd = end

	return true
}

// ranges":"(hit,total);(start,end)
func extractRanges(mm map[string]string, item *Item) bool {
	str, ok := mm["ranges"]
	if !ok {
		fmt.Println("no ranges")
		return false
	}

	if len(str) == 0 {
		return true
	}

	strs := strings.Split(str, ";")
	hit, total, err := decodePair(strs[0])
	if err != nil {
		return false
	}

	item.rngHit = hit
	item.rngTotal = total

	start, end, err := decodePair(strs[1])
	if err != nil {
		return false
	}

	item.rngStart = start
	item.rngEnd = end

	return true
}

// "cnCommit":"(1705484589480577062,1705484589480592418)"
func extractCNCommit(mm map[string]string, item *Item) bool {
	str, ok := mm["cnCommit"]
	if !ok {
		fmt.Println("no cnCommit")
		return false
	}
	start, end, err := decodePair(str)
	if err != nil {
		return false
	}

	item.cnCommitStart = start
	item.cnCommitEnd = end
	return true
}

func prepare(line string) (map[string]string, bool) {
	idx := strings.Index(line, "{")
	if idx == -1 {
		return nil, false
	}

	line = line[idx:]
	mm := map[string]string{}

	fmt.Println(line)
	if err := json.Unmarshal([]byte(line), &mm); err != nil {
		fmt.Println("decode line err", err.Error())
		return nil, false
	}

	return mm, true
}

func lineExtractor(lineStream chan string, logCh chan *Item) {
	for {
	startToReceiveNewLine:
		select {
		case line, ok := <-lineStream:
			if !ok {
				return
			}

			mm, ok := prepare(line)
			if !ok {
				goto startToReceiveNewLine
			}

			item := itemPool.Get().(*Item)

			if ok = extractPhase(mm, item); !ok {
				goto startToReceiveNewLine
			}

			if ok = extractTxnLifeSpan(mm, item); !ok {
				goto startToReceiveNewLine
			}

			if ok = extractCNCommit(mm, item); !ok {
				goto startToReceiveNewLine
			}

			//if ok = extractRanges(mm, item); ok {
			//	goto startToReceiveNewLine
			//}

			logCh <- item
		}
	}
}

const pixel int64 = 200

func logger(logCh chan *Item) {
	file, err := os.Create("draw.txt")
	if err != nil {
		fmt.Println("create draw.txt failed", err.Error())
		return
	}

	defer func() {
		file.Sync()
		file.Close()
	}()

	for {
		select {
		case item, ok := <-logCh:
			if !ok {
				return
			}

			idx := 0
			phases := make([]string, len(item.phaseMap))
			for ph := range item.phaseMap {
				phases[idx] = ph
				idx++
			}

			sort.Slice(phases, func(i, j int) bool {
				if len(item.phaseMap[phases[i]]) == 0 {
					return true
				}

				if len(item.phaseMap[phases[j]]) == 0 {
					return false
				}

				// (start,end),(start,end),...
				// check the first start
				return item.phaseMap[phases[i]][0][0] < item.phaseMap[phases[i]][0][0]
			})

			div := (item.txnEnd - item.txnStart + 1) / pixel
			file.WriteString("|")
			for idx := int64(0); idx < pixel; idx++ {
				file.WriteString("-")
			}

			file.WriteString("|\ttxn life span")

			for idx := 0; idx < len(phases); idx++ {
				file.WriteString("\n")
				durs := item.phaseMap[phases[idx]]
				for x := range durs {
					start, end := durs[x][0], durs[x][1]
					blank := (start - item.txnStart + 1) / div
					for y := int64(0); y < blank; y++ {
						file.WriteString(" ")
					}
					file.WriteString("|")
					ll := (end - start + 1) / div
					for y := int64(1); y < ll; y++ {
						file.WriteString("-")
					}
					file.WriteString("|")
				}
				file.WriteString(fmt.Sprintf("\t%s", phases[idx]))
			}

			file.WriteString("\n\n\n")

			itemPool.Put(item)
		}
	}
}

func Test_Main(t *testing.T) {
	file, err := os.Open("slow_log.txt")
	if err != nil {
		fmt.Println("open slow_log.txt failed", err.Error())
	}

	scanner := bufio.NewScanner(file)

	const maxCapacity int = 1024 * 1024 * 1000
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	lineStream := make(chan string)
	loggerCh := make(chan *Item, 1000*100)

	go lineExtractor(lineStream, loggerCh)
	go logger(loggerCh)

	for scanner.Scan() {
		lineStream <- scanner.Text()
	}

	time.Sleep(time.Second * 5)

	close(lineStream)
	close(loggerCh)

	time.Sleep(time.Second * 5)
}
