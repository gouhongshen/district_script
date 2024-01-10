package insert_benchmark

import (
	"flag"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

type NoPKTable struct {
	A int64
	B int64
	C int64
}

type SinglePKTable struct {
	A int64 `gorm:"primaryKey"`
	B int64
	C int64
}

type ClusterPKTable struct {
	A int64 `gorm:"primaryKey"`
	B int64 `gorm:"primaryKey"`
	C int64
}

var terminals = flag.Int("terminals", 1, "parallel terminals to test")
var sessions = flag.Int("sessions", 1, "sessions cnt to test")
var withPK = flag.Int("withPK", 0, "")
var withTxn = flag.Int("withTXN", 0, "")
var keepTbl = flag.Int("keepTbl", 0, "")
var insSize = flag.Int("insSize", 1000*10*3, "")

const dbname string = "standalone_insert_db"

func connect2DB() *gorm.DB {
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:6001)/%s?charset=utf8mb4&parseTime=True&loc=Local", dbname)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error)})
	if err != nil {
		panic("failed to connect to database!")
	}
	return db
}

func createNoPKTable() *gorm.DB {
	db := connect2DB()
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
	db := connect2DB()
	//if err := db.AutoMigrate(&SinglePKTable{}); err != nil {
	//	panic("failed to create table")
	//}

	if *keepTbl <= 0 {
		db.Exec("drop table single_pk_tables")
		db.Exec("create table single_pk_tables (a bigint, b bigint, c bigint, primary key (`a`))")
	}

	return db
}

func generateValues(s int, e int, offset int64) (string, time.Duration) {
	start := time.Now()
	var values []string
	for idx := s; idx < e; idx++ {
		a := int64(idx) + offset
		b := 2 * a
		c := 3 * a
		values = append(values, fmt.Sprintf("(%d,%d,%d)", a, b, c))
	}
	return strings.Join(values, ","), time.Since(start)
}

func insertJob(ses []*gorm.DB, left, right int, tblName string, wg *sync.WaitGroup) {
	startIdx := int64(0)
	if *keepTbl > 0 {
		ses[0].Table(tblName).Count(&startIdx)
		startIdx *= 2
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

			ses[rand.Int()%len(ses)].Exec(fmt.Sprintf("insert into %s values %s;", tblName, values))
		}

		values, dur := generateValues(idx, right, startIdx)
		noiseDur += dur

		ses[rand.Int()%len(ses)].Exec(fmt.Sprintf("insert into %s values %s;", tblName, values))

		realDur := (time.Since(start) - noiseDur).Seconds()
		fmt.Printf("no pk table insert %d rows (with txn %d) done, takes %6.3f s, %6.3f ms/txn(%d values)\n",
			totalRows, step, realDur, realDur*1000/(float64(totalRows)/float64(step)), step)

	} else {
		for idx := left; idx < right; idx++ {
			a := int64(idx) + startIdx
			b := 2 * a
			c := 3 * a
			ses[rand.Int()%len(ses)].Exec(fmt.Sprintf("insert into %s values(?, ?, ?);", tblName), a, b, c)
		}

		realDur := (time.Since(start) - noiseDur).Seconds()
		fmt.Printf("no pk table insert %d rows done, takes %6.3f s, %6.3f ms/txn(%d values)\n",
			totalRows, realDur, realDur*1000/(float64(totalRows)), 1)
	}

	wg.Done()
}

func InsertWorker(db *gorm.DB, tblName string) {
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
		go insertJob(ses, left, right, tblName, &wg)
	}

	wg.Wait()

}

func Test_Main(t *testing.T) {
	flag.Parse()
	fmt.Printf(
		"terminals: %d, sessions: %d, withPK: %d, withTxn: %d, keepTbl: %d, insSize %dW\n",
		*terminals, *sessions, *withPK, *withTxn, *keepTbl, (*insSize)/10000)
	fmt.Printf("start: %s\n", time.Now().Local())

	if *withPK > 0 {
		Test_SinglePKInsert(t)
	} else {
		Test_NoPKInsert(t)

	}
	fmt.Println("end: ", time.Now().Local())
}

func Test_NoPKInsert(t *testing.T) {
	db := createNoPKTable()
	InsertWorker(db, "no_pk_tables")
}

func Test_SinglePKInsert(t *testing.T) {
	db := createSinglePKTable()
	InsertWorker(db, "single_pk_tables")
}
