package main

import (
	"context"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"math/rand"
	"sync"
	"time"
)

type District struct {
	Wid  int32 `gorm:"primaryKey"`
	Did  int32 `gorm:"primaryKey"`
	Next int64
}

func CreateTable() *gorm.DB {
	dsn := "dump:111@tcp(127.0.0.1:6001)/fake_tpcc?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error)})
	if err != nil {
		panic("failed to connect to database!")
	}

	if err = db.AutoMigrate(&District{}); err != nil {
		panic("failed to create table")
	}

	return db
}

func RunUpdateSql(db *gorm.DB) {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	wid := rand.Int31()%10 + 1
	did := rand.Int31()%10 + 1

	db.Transaction(func(tx *gorm.DB) error {
		tx.Model(&District{}).
			Where("wid = ? and did = ?", wid, did).
			UpdateColumn("`next`", gorm.Expr("`next` + ?", 1))
		return nil
	})

}

func LaunchCheckWorker(ctx context.Context, wg *sync.WaitGroup, db *gorm.DB) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ses := db.Session(&gorm.Session{PrepareStmt: true})
		ticker := time.NewTicker(time.Minute)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var result []struct {
					Wid uint
					Did uint
					Cnt int
				}

				if err := ses.Table("districts").
					Select("wid, did, COUNT(*) as cnt").
					Group("wid, did").
					Order("cnt desc").
					Limit(1).
					Scan(&result).Error; err != nil {
					log.Fatalf("Failed to execute query: %v", err)
				}

				if result[0].Cnt > 1 {
					fmt.Println("consistency failed: ", result[0].Wid, result[0].Did, result[0].Cnt)
				}
			}
		}
	}()

}

func LaunchUpdateWorker(ctx context.Context, wg *sync.WaitGroup, db *gorm.DB) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ses := db.Session(&gorm.Session{PrepareStmt: true})
		ticker := time.NewTicker(time.Microsecond)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				RunUpdateSql(ses)
			}
		}
	}()

}

func InitTable(db *gorm.DB) {
	for wid := 1; wid <= 10; wid++ {
		for did := 1; did <= 10; did++ {
			db.Save(District{
				Wid: int32(wid),
				Did: int32(did),
			})
		}
	}
}

func main() {
	db := CreateTable()
	InitTable(db)

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*30)
	defer cancel()

	for idx := 0; idx < 100; idx++ {
		LaunchUpdateWorker(ctx, &wg, db)
	}

	LaunchCheckWorker(ctx, &wg, db)

	wg.Wait()

	fmt.Println("script done")
}
