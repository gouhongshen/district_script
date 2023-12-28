package bvt

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/rand"
	"testing"
	"time"
)

type TimeStamp struct {
	A time.Time
	B time.Time
}

func Connect2Database() *gorm.DB {
	dsn := "dump:111@tcp(127.0.0.1:6001)/fake_bvt?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error)})
	if err != nil {
		panic("failed to connect to database!")
	}

	//if err = db.AutoMigrate(&TimeStamp{}); err != nil {
	//	panic("failed to create table")
	//}

	return db
}

func LaunchInsertWorker(db *gorm.DB) {
	db.Exec("create table timestamp(a date, b date)")

	sessions := make([]*gorm.DB, 4)
	for idx := range sessions {
		sessions[idx] = db.Session(&gorm.Session{PrepareStmt: false})
	}

	sqls := []string{
		"insert into timestamp values('2019-11-01 12:15:12', '2018-01-01 12:15:12');",
		"insert into timestamp values('2019-10-01 12:15:12', '2018-01-01 12:15:12');",
		"insert into timestamp values('2020-10-01 12:15:12', '2018-01-01 12:15:12');",
		"insert into timestamp values('2021-11-01 12:15:12', '2018-01-01 12:15:12');",
		"insert into timestamp values('2022-01-01 12:15:12', '2018-01-01 12:15:12');",
		"insert into timestamp values('2018-01-01 12:15:12', '2019-11-01 12:15:12');",
		"insert into timestamp values( '2018-01-01 12:15:12', '2019-10-01 12:15:12');",
		"insert into timestamp values( '2018-01-01 12:15:12', '2020-10-01 12:15:12');",
		"insert into timestamp values( '2018-01-01 12:15:12', '2021-11-01 12:15:12');",
		"insert into timestamp values( '2018-01-01 12:15:12', '2022-01-01 12:15:12');",
		"SELECT a, b, TIMESTAMPDIFF(MINUTE, a, b) from timestamp;",
	}

	sqlsIdx := 0

	deadline := time.NewTimer(time.Hour * 10)
	for {
		select {
		case <-deadline.C:
			fmt.Println("exit insert worker")
		default:
			if sqlsIdx == len(sqls) {
				//time.Sleep(time.Second * 1)
				fmt.Println("start next round")
				db.Exec("drop table timestamp")
				db.Exec("create table timestamp(a date, b date)")
				sqlsIdx = 0
				break
			}

			ses := sessions[rand.Int()%len(sessions)]
			ses.Exec(sqls[sqlsIdx])
			sqlsIdx++
		}
	}
}

func Test_Main(t *testing.T) {
	db := Connect2Database()
	LaunchInsertWorker(db)
}
