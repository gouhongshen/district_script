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

/*

+-------------+---------------+------+------+---------+-------+---------+
| Field       | Type          | Null | Key  | Default | Extra | Comment |
+-------------+---------------+------+------+---------+-------+---------+
| d_w_id      | INT(32)       | NO   | PRI  | NULL    |       |         |
| d_id        | INT(32)       | NO   | PRI  | NULL    |       |         |
| d_ytd       | DECIMAL64(12) | YES  |      | NULL    |       |         |
| d_tax       | DECIMAL64(4)  | YES  |      | NULL    |       |         |
| d_next_o_id | INT(32)       | YES  |      | NULL    |       |         |
| d_name      | VARCHAR(10)   | YES  |      | NULL    |       |         |
| d_street_1  | VARCHAR(20)   | YES  |      | NULL    |       |         |
| d_street_2  | VARCHAR(20)   | YES  |      | NULL    |       |         |
| d_city      | VARCHAR(20)   | YES  |      | NULL    |       |         |
| d_state     | CHAR(2)       | YES  |      | NULL    |       |         |
| d_zip       | CHAR(9)       | YES  |      | NULL    |       |         |
+-------------+---------------+------+------+---------+-------+---------+
*/

type Bmsql_District struct {
	D_W_Id      int32 `gorm:"primaryKey"`
	D_Id        int32 `gorm:"primaryKey"`
	D_YTD       int64
	D_TAX       int64
	D_Next_O_Id int32
	D_Name      string
	D_Street_1  string
	D_Street_2  string
	D_City      string
	D_State     string
	D_Zip       string
}

func CreateTable() *gorm.DB {
	dsn := "dump:111@tcp(127.0.0.1:6001)/fake_tpcc?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error)})
	if err != nil {
		panic("failed to connect to database!")
	}

	if err = db.AutoMigrate(&Bmsql_District{}); err != nil {
		panic("failed to create table")
	}

	return db
}

func RunUpdateSql(db *gorm.DB) {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	wid := rand.Int31()%10 + 1
	did := rand.Int31()%10 + 1

	db.Transaction(func(tx *gorm.DB) error {
		tx.Model(&Bmsql_District{}).
			Where("d_w_id = ? and d_id = ?", wid, did).
			UpdateColumn("`d_next_o_id`", gorm.Expr("`d_next_o_id` + ?", 1))
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
					D_W_Id uint
					D_Id   uint
					Cnt    int
				}

				if err := ses.Table("bmsql_districts").
					Select("d_w_id, d_id, COUNT(*) as cnt").
					Group("d_w_id, d_id").
					Order("cnt desc").
					Limit(1).
					Scan(&result).Error; err != nil {
					log.Fatalf("Failed to execute query: %v", err)
				}

				fmt.Println(result[0])
				if result[0].Cnt > 1 {
					fmt.Println("consistency failed: ", result[0].D_W_Id, result[0].D_Id, result[0].Cnt)
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
		//ticker := time.NewTicker(time.Microsecond)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				RunUpdateSql(ses)
			}
		}
	}()
}

func InitTable(db *gorm.DB) {
	for wid := 1; wid <= 10; wid++ {
		for did := 1; did <= 10; did++ {
			db.Save(Bmsql_District{
				D_W_Id: int32(wid),
				D_Id:   int32(did),
			})
		}
	}
}

func main() {
	db := CreateTable()
	InitTable(db)

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour*30)
	defer cancel()

	fmt.Println("start creating worker...")
	for idx := 0; idx < 100; idx++ {
		LaunchUpdateWorker(ctx, &wg, db)
	}

	LaunchCheckWorker(ctx, &wg, db)

	fmt.Println("all worker created done.")

	wg.Wait()

	fmt.Println("script done")
}
