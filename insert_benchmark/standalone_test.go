package insert_benchmark

import (
	"flag"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/rand"
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
var tblSize = flag.Int("tblSize", 1000*10*3, "")

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

func insertJob(ses []*gorm.DB, left, right int, tblName string, wg *sync.WaitGroup) {
	start := time.Now()
	totalRows := right - left + 1
	if *withTxn > 0 {
		step := *withTxn

		idx := left
		for ; idx+step < right; idx += step {
			txn := ses[rand.Int()%len(ses)].Begin()
			for x := idx; x < idx+step; x++ {
				txn.Exec(fmt.Sprintf("insert into %s values(?, ?, ?);", tblName), x, x*2, x*3)
			}
			txn.Commit()
		}

		txn := ses[rand.Int()%len(ses)].Begin()
		for ; idx < right; idx++ {
			txn.Exec(fmt.Sprintf("insert into %s values(?, ?, ?);", tblName), idx, idx*2, idx*3)
		}
		txn.Commit()

		fmt.Printf("no pk table insert %d rows (with txn %d) done, takes %f s\n", totalRows, step, time.Since(start).Seconds())

	} else {
		for idx := left; idx < right; idx++ {
			ses[rand.Int()%len(ses)].Exec(fmt.Sprintf("insert into %s values(?, ?, ?);", tblName), idx, idx*2, idx*3)
		}
		fmt.Printf("no pk table insert %d rows done, takes %f s\n", totalRows, time.Since(start).Seconds())
	}

	wg.Done()
}

func InsertWorker(db *gorm.DB, tblName string) {
	var ses []*gorm.DB
	for idx := 0; idx < *sessions; idx++ {
		ses = append(ses, db.Session(&gorm.Session{PrepareStmt: false}))
	}

	maxRows := *tblSize
	step := maxRows / *terminals

	var wg sync.WaitGroup
	for idx := 0; idx < *terminals; idx++ {
		wg.Add(1)
		go insertJob(ses, idx*step, idx*step+step, tblName, &wg)
	}

	wg.Wait()

}

func Test_Main(t *testing.T) {
	flag.Parse()
	fmt.Printf(
		"terminals: %d, sessions: %d, withPK: %d, withTxn: %d, keepTbl: %d, tblSize %dW\n",
		*terminals, *sessions, *withPK, *withTxn, *keepTbl, (*tblSize)/10000)
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