package common

import (
	"encoding/csv"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"gorm.io/gorm"
	"os"
	"strings"
	"sync"
	"time"
)

var waitLoadChan = make(chan [][]string, 1)

func dataWorker(
	totalRows int, offset int64, wg *sync.WaitGroup,
	valGenerator func(s int, e int, offset int64) (string, time.Duration)) {
	left := totalRows
	s := 0
	for left > 0 {
		batch := 1000 * 10
		if batch > left {
			batch = left
		}

		res, _ := valGenerator(s, s+batch, offset)
		values := strings.Split(res, "),")

		formated := make([][]string, batch)
		for idx := 0; idx < len(values); idx++ {
			if len(values[idx]) == 0 {
				continue
			}

			str := values[idx][1:]
			if str[len(str)-1] == ')' {
				str = str[:len(str)-1]
			}

			formated[idx] = strings.Split(str, ",")
		}

		waitLoadChan <- formated

		s += batch
		left -= batch
	}

	wg.Done()
}

func LoadData2Table(
	db *gorm.DB,
	rows int, dbName, tblName string,
	valGenerator func(s int, e int, offset int64) (string, time.Duration)) (error, time.Duration) {

	start := time.Now()

	file, err := os.Create("tmp_for_load.csv")
	if err != nil {
		fmt.Println("create tmp csv file for load failed", err.Error())
		return err, time.Since(start)
	}

	defer func() {
		if err = os.Remove(file.Name()); err != nil {
			fmt.Println("remove tmp file failed", err.Error())
		}
	}()

	csvWriter := csv.NewWriter(file)

	// to avoid oom, split rows into groups, each group has 100W rows
	workerPool, err := ants.NewPool(10)
	if err != nil {
		fmt.Println("create worker pool failed", err.Error())
		return err, time.Since(start)
	}

	wg := sync.WaitGroup{}
	workerNum := 10
	wg.Add(workerNum)

	split := rows / 10
	for idx := 0; idx < workerNum; idx++ {
		s := idx * split
		cnt := split
		if s+cnt > rows {
			cnt = rows - s
		}

		workerPool.Submit(func() {
			dataWorker(cnt, int64(s), &wg, valGenerator)
		})
	}

	received := 0
	for received < rows {
		res := <-waitLoadChan
		if err = csvWriter.WriteAll(res); err != nil {
			fmt.Println("write record to csv failed", err.Error())
			return err, time.Since(start)
		}
		received += len(res)
	}

	wg.Wait()

	csvWriter.Flush()
	file.Sync()
	file.Close()

	absPath, err := os.Getwd()
	if err != nil {
		fmt.Println("get current path failed", err.Error())
		return err, time.Since(start)
	}

	absPath += "/" + file.Name()

	db.Exec(fmt.Sprintf("load data infile '%s' into table `%s`.`%s` fields terminated by ',' lines terminated by '\n' parallel 'true';;",
		absPath, dbName, tblName))

	return nil, time.Since(start)
}
