package insert_benchmark

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

type NoPKTable struct {
	A int64
	B int64
	C int64
	D string
	E int64
}

type SinglePKTable struct {
	A int64 `gorm:"primaryKey"`
	B int64
	C int64
	D string
	E int64
}

type ClusterPKTable struct {
	A int64 `gorm:"primaryKey"`
	B int64 `gorm:"primaryKey"`
	C int64
	D string
	E int64
}

var terminals = flag.Int("terminals", 5, "parallel terminals to test")
var sessions = flag.Int("sessions", 5, "sessions cnt to test")
var withPK = flag.Int("withPK", 0, "")
var withTxn = flag.Int("withTXN", 1000, "")
var keepTbl = flag.Int("keepTbl", 0, "")
var insSize = flag.Int("insSize", 1000*1000, "")

var latencyDir string = "./latency_recorder"
var tracePProfDir string = "./trace_pprof"

const standaloneInsertDB string = "standalone_insert_db"

func connect2DB(dbname string) *gorm.DB {
	dsn := fmt.Sprintf("dump:f56TEhwiswWu@tcp(127.0.0.1:6003)/%s?charset=utf8mb4&parseTime=True&loc=Local", dbname)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error)})
	if err != nil {
		panic("failed to connect to database!")
	}
	return db
}

func createNoPKTable() *gorm.DB {
	db := connect2DB(standaloneInsertDB)
	//if err := db.AutoMigrate(&NoPKTable{}); err != nil {
	//	panic("failed to create table")
	//}
	if *keepTbl <= 0 {
		db.Exec("drop table no_pk_tables")
		db.Exec("create table no_pk_tables (a bigint, b bigint, c bigint)")
	}

	return db
}

func createSinglePKTable() *gorm.DB {
	db := connect2DB(standaloneInsertDB)
	//if err := db.AutoMigrate(&SinglePKTable{}); err != nil {
	//	panic("failed to create table")
	//}

	if *keepTbl <= 0 {
		db.Exec("drop table single_pk_tables")
		db.Exec("create table single_pk_tables (a bigint, b bigint, c bigint, primary key (`a`))")
	}

	return db
}

func createClusterPKTable() *gorm.DB {
	db := connect2DB(standaloneInsertDB)
	//if err := db.AutoMigrate(&SinglePKTable{}); err != nil {
	//	panic("failed to create table")
	//}

	if *keepTbl <= 0 {
		db.Exec("drop table cluster_pk_tables")
		time.Sleep(time.Second)
		db.Exec("create table cluster_pk_tables (a bigint, b bigint, c bigint, primary key (`a`, `b`))")
	}

	return db
}

func defaultGenerateValues(s int, e int, offset int64) (string, time.Duration) {
	start := time.Now()
	var values []string
	for idx := s; idx < e; idx++ {
		a := int64(idx) + offset
		b := 2 * a
		c := 3 * a
		d := uuid.New().String()
		e := 4 * a
		values = append(values, fmt.Sprintf("(%d,%d,%d,'%s',%d)", a, b, c, d, e))
	}
	return strings.Join(values, ","), time.Since(start)
}

type latencyRecorder struct {
	OpCounter, RecorderStep int64
	LatencyMap              map[int64]int64
}

var recorders []*latencyRecorder

func newLatencyRecorder(cnt int, step int) []*latencyRecorder {
	recorders := make([]*latencyRecorder, cnt)
	for idx := 0; idx < cnt; idx++ {
		recorders[idx] = &latencyRecorder{
			OpCounter:    int64(0),
			RecorderStep: int64(step),
			LatencyMap:   make(map[int64]int64),
		}
	}

	return recorders
}

func syncLatency(tblName string, op string) {
	// -withPK 2 -terminals 50 -sessions 50 -withTXN 10000 -keepTbl 0 -insSize 100000000
	fileName := fmt.Sprintf("%s/%s_%s_pk%d_ter%d_ses%d_txn%d_keep%d_insSize%dw_%.5f",
		latencyDir,
		tblName, op, *withPK, *terminals, *sessions, *withTxn, *keepTbl, *insSize/10000,
		float64(time.Now().Unix())/(60*60))

	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("sync latency failed: ", err.Error())
	}

	for idx := range recorders {
		file.WriteString(fmt.Sprintf("terminal: %d, %s %d, %d each time\n",
			idx, op, recorders[idx].OpCounter, *withTxn))

		latency := make([]int64, len(recorders[idx].LatencyMap))
		for k, v := range recorders[idx].LatencyMap {
			latency[int(k)] = v
		}

		acc := int(0)
		for k := range latency {
			start := k * int(recorders[idx].RecorderStep)
			end := start + int(recorders[idx].RecorderStep)

			opCnt := int(recorders[idx].RecorderStep)
			if k == len(latency)-1 {
				opCnt = int(recorders[idx].OpCounter) - acc
			}
			acc += opCnt

			avg := float64(latency[k]) / float64(opCnt)
			file.WriteString(fmt.Sprintf("[%d_%d): %12.6fms\n", start, end, avg))
		}

		file.WriteString("\n")
	}

	file.Sync()
	file.Close()
}

func insertHelper(db *gorm.DB, tblName string, values string, jobId int) {
	start := time.Now()
	rr := recorders[jobId]

	db.Exec(fmt.Sprintf("insert into %s values %s;", tblName, values))

	dur := time.Since(start).Milliseconds()

	rr.LatencyMap[rr.OpCounter/rr.RecorderStep] += dur
	rr.OpCounter++
}

func insertJob(
	ses []*gorm.DB, left, right int,
	tblName string, wg *sync.WaitGroup,
	jobId int, generateValues func(s, e int, offset int64) (string, time.Duration)) {
	startIdx := int64(0)
	if *keepTbl > 0 {
		ses[0].Table(tblName).Count(&startIdx)
		startIdx *= 2
	}

	if generateValues == nil {
		generateValues = defaultGenerateValues
	}

	noiseDur := time.Duration(0)
	start := time.Now()
	totalRows := right - left

	if *withTxn > 0 {
		step := *withTxn

		idx := left
		for ; idx+step < right; idx += step {
			values, dur := generateValues(idx, idx+step, startIdx)
			noiseDur += dur

			insertHelper(ses[rand.Int()%len(ses)], tblName, values, jobId)
		}

		values, dur := generateValues(idx, right, startIdx)
		noiseDur += dur

		insertHelper(ses[rand.Int()%len(ses)], tblName, values, jobId)

		realDur := (time.Since(start) - noiseDur).Seconds()
		fmt.Printf("insert into %s %d rows (with txn %d) done, takes %6.3f s, %6.3f ms/txn(%d values)\n",
			tblName, totalRows, step, realDur, realDur*1000/(float64(totalRows)/float64(step)), step)

	} else {
		for idx := left; idx < right; idx++ {
			values, dur := generateValues(idx, idx+1, startIdx)
			noiseDur += dur

			insertHelper(ses[rand.Int()%len(ses)], tblName, values, jobId)
		}

		realDur := (time.Since(start) - noiseDur).Seconds()
		fmt.Printf("insert into %s %d rows done, takes %6.3f s, %6.3f ms/txn(%d values)\n",
			tblName, totalRows, realDur, realDur*1000/(float64(totalRows)), 1)
	}

	wg.Done()
}

func InsertWorker(
	db *gorm.DB, tblName string,
	generateValues func(int, int, int64) (string, time.Duration)) {
	recorders = newLatencyRecorder(*terminals, 100)

	os.Mkdir(tracePProfDir, 0666)
	os.Mkdir(latencyDir, 0666)

	var ses []*gorm.DB
	for idx := 0; idx < *sessions; idx++ {
		ses = append(ses, db.Session(&gorm.Session{PrepareStmt: false}))
	}

	maxRows := *insSize
	step := maxRows / *terminals

	var wg sync.WaitGroup
	for idx := 0; idx < *terminals; idx++ {
		wg.Add(1)
		left := idx * step
		right := idx*step + step

		if idx == *terminals-1 {
			right = maxRows
		}
		go insertJob(ses, left, right, tblName, &wg, idx, generateValues)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	go tracePProfWorker(ctx, ch)

	wg.Wait()
	cancel()

	<-ch

	syncLatency(tblName, "insert")
}

func tracePProfWorker(ctx context.Context, ch chan struct{}) {
	id := 0
	wg := sync.WaitGroup{}
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ctx.Done():
			ch <- struct{}{}
			return
		case <-ticker.C:
			wg.Add(2)
			go func() {
				name := fmt.Sprintf("%s/trace_(pk)%d_(txn)%d_(keep)%d_%02d.out",
					tracePProfDir, *withPK, *withTxn, *keepTbl, id)
				cmd := exec.Command("curl", "-o", name, "http://127.0.0.1:6060/debug/pprof/trace?seconds=30")

				if err := cmd.Run(); err != nil {
					fmt.Println(err.Error())
				}
				wg.Done()
			}()

			go func() {
				name := fmt.Sprintf("%s/profile_(pk)%d_(txn)%d_(keep)%d_%02d.out",
					tracePProfDir, *withPK, *withTxn, *keepTbl, id)
				cmd := exec.Command("curl", "-o", name, "http://127.0.0.1:6060/debug/pprof/profile?seconds=30")
				if err := cmd.Run(); err != nil {
					fmt.Println(err.Error())
				}
				wg.Done()
			}()

			wg.Wait()
			id++
			ticker.Reset(time.Second * 30)
		}
	}
}

func Test_Main(t *testing.T) {
	flag.Parse()
	fmt.Printf(
		"terminals: %d, sessions: %d, withPK: %d, withTxn: %d, keepTbl: %d, insSize %dW\n",
		*terminals, *sessions, *withPK, *withTxn, *keepTbl, (*insSize)/10000)
	fmt.Printf("start: %s\n", time.Now().Local())

	if *withPK == 2 {
		Test_ClusterPKInsert(t)
	} else if *withPK == 1 {
		Test_SinglePKInsert(t)
	} else {
		Test_NoPKInsert(t)

	}
	fmt.Println("end: ", time.Now().Local())
}

func Test_NoPKInsert(t *testing.T) {
	db := createNoPKTable()
	InsertWorker(db, "no_pk_tables", nil)
}

func Test_SinglePKInsert(t *testing.T) {
	db := createSinglePKTable()
	InsertWorker(db, "single_pk_tables", nil)
}

func Test_ClusterPKInsert(t *testing.T) {
	db := createClusterPKTable()
	InsertWorker(db, "cluster_pk_tables", nil)
}

func Test_Statement_CU_Insert(t *testing.T) {
	db := connect2DB("mo_catalog")
	InsertWorker(db, "statement_cu_for_test", generateStatementCUValues)
}

/*

+--------------+------------------+------+------+---------+-------+--------------------------------+
| Field        | Type             | Null | Key  | Default | Extra | Comment                        |
+--------------+------------------+------+------+---------+-------+--------------------------------+
| statement_id | VARCHAR(36)      | NO   | PRI  | NULL    |       |                                |
| account      | VARCHAR(300)     | NO   |      | NULL    |       |                                |
| response_at  | DATETIME(0)      | YES  |      | null    |       |                                |
| cu           | DECIMAL128(23)   | NO   |      | NULL    |       |                                |
| account_id   | INT UNSIGNED(32) | NO   | PRI  | NULL    |       | the account_id added by the mo |
+--------------+------------------+------+------+---------+-------+--------------------------------+
*/

func generateStatementCUValues(s, e int, offset int64) (string, time.Duration) {
	start := time.Now()
	var values []string
	for idx := s; idx < e; idx++ {
		stmtId := uuid.New().String()
		account := "account"
		responseAt := "2024-01-17 06:23:34.861392517"
		cu := 0
		accountId := int64(idx) + offset

		values = append(values,
			fmt.Sprintf("('%s','%s','%s',%d,%d)", stmtId, account, responseAt, cu, accountId))
	}
	return strings.Join(values, ","), time.Since(start)
}
