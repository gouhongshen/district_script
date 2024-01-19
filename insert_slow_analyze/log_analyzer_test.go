package insert_slow_analyze

import (
	"bufio"
	"bytes"
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
	phase, ok := mm["operator duration"]
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

func extractPrepare(line string) (map[string]string, bool) {
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

func lineExtractor(lineStream chan string, logCh chan *Item, wg *sync.WaitGroup) {
	defer func() {
		close(logCh)
		wg.Done()
	}()

	for {
	startToReceiveNewLine:
		select {
		case line, ok := <-lineStream:
			if !ok {
				return
			}

			mm, ok := extractPrepare(line)
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

			if ok = extractRanges(mm, item); !ok {
				goto startToReceiveNewLine
			}

			logCh <- item
		}
	}
}

const txnLifeSpanFmtLength int64 = 120

func fmtLogHeader() string {
	str := "\n\n\n"
	for idx := int64(0); idx < txnLifeSpanFmtLength; idx++ {
		str += "*"
	}
	str += "\n\n\n"
	return str
}

func sortedOpNames(item *Item) []string {
	idx := 0
	opNames := make([]string, len(item.phaseMap))
	for ph := range item.phaseMap {
		opNames[idx] = ph
		idx++
	}

	sort.Slice(opNames, func(i, j int) bool {
		if len(item.phaseMap[opNames[i]]) == 0 {
			return true
		}

		if len(item.phaseMap[opNames[j]]) == 0 {
			return false
		}

		// (start,end),(start,end),...
		// check the first start
		return item.phaseMap[opNames[i]][0][0] < item.phaseMap[opNames[i]][0][0]
	})

	return opNames
}

func fmtRanges(item *Item) string {
	return fmt.Sprintf("ranges: %d/%d=%.3f, taken: %dms\n",
		item.rngHit, item.rngTotal, float64(item.rngHit)/float64(item.rngTotal),
		time.UnixMicro(item.rngEnd).Sub(time.UnixMicro(item.rngStart)).Milliseconds())
}

func fmtCNCommit(item *Item) string {
	return fmt.Sprintf("cn commit: taken: %dms\n\n",
		time.Unix(0, item.cnCommitEnd).Sub(time.Unix(0, item.cnCommitStart)).Milliseconds())
}

func fmtTxnLifeSpan(item *Item) string {

	var buf bytes.Buffer

	buf.WriteString("|")
	for idx := int64(2); idx < txnLifeSpanFmtLength; idx++ {
		buf.WriteString("-")
	}

	buf.WriteString(fmt.Sprintf("|\t\ttxn life span(%dms)\n", item.txnEnd-item.txnEnd))

	return buf.String()
}

func fmtOpLifeSpan(opName string, item *Item) string {
	offset := item.txnStart
	txnLifeSpan := float64(item.txnEnd - item.txnStart)

	var lifeSpan int64
	var buf bytes.Buffer
	segs := item.phaseMap[opName]

	for idx := 0; idx < len(segs); idx++ {
		interval := segs[idx]
		blankLen := int(float64(interval[0]-offset) / txnLifeSpan * float64(txnLifeSpanFmtLength))

		for x := 0; x < blankLen; x++ {
			buf.WriteString(" ")
		}

		duration := interval[1] - interval[0]
		lifeSpan += duration
		dashLen := int(float64(duration)/txnLifeSpan*float64(txnLifeSpanFmtLength)) - 2

		if duration > 1000 {
			buf.WriteString("|")
			for x := 0; x < dashLen; x++ {
				buf.WriteString("-")
			}
			buf.WriteString("|")
		}

		offset = interval[1]
	}

	buf.WriteString(fmt.Sprintf("\t\t%s(%dms)\n", opName, lifeSpan))

	return buf.String()
}

func logger(logCh chan *Item, wg *sync.WaitGroup) {
	file, err := os.Create("draw.txt")
	if err != nil {
		fmt.Println("create draw.txt failed", err.Error())
		return
	}

	defer func() {
		file.Sync()
		file.Close()
		wg.Done()
	}()

	for {
		select {
		case item, ok := <-logCh:
			if !ok {
				return
			}

			file.WriteString(fmtLogHeader())
			file.WriteString(fmtRanges(item))
			file.WriteString(fmtCNCommit(item))
			file.WriteString(fmtTxnLifeSpan(item))

			opNames := sortedOpNames(item)
			for idx := 0; idx < len(opNames); idx++ {
				file.WriteString(fmtOpLifeSpan(opNames[idx], item))
			}

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

	wg := sync.WaitGroup{}
	wg.Add(2)

	go lineExtractor(lineStream, loggerCh, &wg)
	go logger(loggerCh, &wg)

	for scanner.Scan() {
		lineStream <- scanner.Text()
	}

	close(lineStream)
	wg.Wait()
}
