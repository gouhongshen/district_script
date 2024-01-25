package insert_benchmark

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strconv"
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

var terminals = flag.Int("terminals", 1, "parallel terminals to test")
var sessions = flag.Int("sessions", 1, "sessions cnt to test")
var withPK = flag.Int("withPK", 1, "")
var withTxn = flag.Int("withTXN", 1000, "")
var keepTbl = flag.Int("keepTbl", 0, "")
var insSize = flag.Int("insSize", 1000*1000, "")

var latencyDir string
var tracePProfDir string

const standaloneInsertDB string = "standalone_insert_db"

func connect2DB(dbname string) *gorm.DB {
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:6001)/%s?charset=utf8mb4&parseTime=True&loc=Local", dbname)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error)})
	if err != nil {
		panic("failed to connect to database!")
	}
	return db
}

func createNoPKTable() *gorm.DB {
	db := connect2DB(standaloneInsertDB)
	if *keepTbl <= 0 {
		db.Exec("drop table no_pk_tables")
		db.Exec("create table no_pk_tables (a bigint, b bigint, c bigint, d varchar, e bigint)")
	}

	return db
}

func createWideMIndexesTable(createSql string) *gorm.DB {
	db := connect2DB("wide_indexes_db")
	if *keepTbl <= 0 {
		db.Exec("drop table wide_indexes_table")
		db.Exec(createSql)
	}

	return db
}

func createSinglePKTable() *gorm.DB {
	db := connect2DB(standaloneInsertDB)
	if *keepTbl <= 0 {
		db.Exec("drop table single_pk_tables")
		db.Exec("create table single_pk_tables (a bigint, b bigint, c bigint, d varchar, e bigint, primary key (`a`))")
	}

	return db
}

func createClusterPKTable() *gorm.DB {
	db := connect2DB(standaloneInsertDB)
	if *keepTbl <= 0 {
		db.Exec("drop table cluster_pk_tables")
		time.Sleep(time.Second)
		db.Exec("create table cluster_pk_tables (a bigint, b bigint, c bigint, d varchar, e bigint, primary key (`a`, `b`))")
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

	dir, _ := os.Getwd()
	latencyDir = dir + "/latency_recorder"
	tracePProfDir = dir + "/trace_pprof"

	os.Mkdir(tracePProfDir, 0777)
	os.Mkdir(latencyDir, 0777)

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
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	go tracePProfWorker(ctx, ch1)
	go cpuMemWorker(ctx, ch2)

	wg.Wait()
	cancel()

	<-ch1
	<-ch2

	syncLatency(tblName, "insert")
}

func cpuMemWorker(ctx context.Context, ch chan struct{}) {
	time.Sleep(time.Second * 10)
	var cpuUsage []float64
	var memUsage []float64

	defer func() {
		sort.Slice(cpuUsage, func(i, j int) bool { return cpuUsage[i] < cpuUsage[j] })
		sort.Slice(memUsage, func(i, j int) bool { return memUsage[i] < memUsage[j] })
		avgcpu, midcpu := 0.0, 0.0
		for idx := range cpuUsage {
			avgcpu += cpuUsage[idx]
		}
		avgcpu = avgcpu / float64(len(cpuUsage))
		midcpu = cpuUsage[len(cpuUsage)/2]

		avgmem, midmem := 0.0, 0.0
		for idx := range memUsage {
			avgmem += memUsage[idx]
		}
		avgmem = avgmem / float64(len(memUsage))
		midmem = memUsage[len(memUsage)/2]

		fmt.Printf("avg-cpu: %.3f/%.3f, mid-cpu: %.3f/%.3f, avg-mem: %.3f/%.3f, mid-mem: %.3f/%.3f\n",
			avgcpu, 100.0, midcpu, 100.0,
			avgmem, 100.0, midmem, 100.0)
	}()

	ticker := time.NewTicker(time.Millisecond * 10)
	for {
		select {
		case <-ctx.Done():
			ch <- struct{}{}
			return

		case <-ticker.C:
			res, err := cpu.Percent(time.Second, false)
			if err != nil {
				fmt.Println("cpu.Percent failed")
			}
			cpuUsage = append(cpuUsage, res[0])

			memStats, err := mem.VirtualMemory()
			if err != nil {
				fmt.Println("mem.VirtualMemory failed")
			}
			memUsage = append(memUsage, memStats.UsedPercent)

			ticker.Reset(time.Millisecond * 10)
		}
	}

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
				cmd := exec.Command("curl", "-o", name, "http://127.0.0.1:6060/debug/pprof/trace?seconds=15")
				if err := cmd.Run(); err != nil {
					fmt.Println(err.Error())
				}
				wg.Done()
			}()

			go func() {
				name := fmt.Sprintf("%s/profile_(pk)%d_(txn)%d_(keep)%d_%02d.out",
					tracePProfDir, *withPK, *withTxn, *keepTbl, id)
				cmd := exec.Command("curl", "-o", name, "http://127.0.0.1:6060/debug/pprof/profile?seconds=15")
				if err := cmd.Run(); err != nil {
					fmt.Println(err.Error())
				}
				wg.Done()
			}()
			wg.Wait()
			id++
			ticker.Reset(time.Second * 15)
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

func Test_WideMIndexedTable_Insert(t *testing.T) {
	createTableSql := "create table wide_indexes_table (" +
		"`id` bigint(0) NOT NULL AUTO_INCREMENT COMMENT '自增主键'," +
		"`company_id` bigint(0) NOT NULL COMMENT '所属企业Id，作为分区字段，与自增长id作为联合主键'," +
		"`code` varchar(32)  NULL DEFAULT NULL COMMENT '车企编码 kafak导入'," +
		"`vccodedosedf` varchar(32)  NULL DEFAULT NULL COMMENT 'vccodedosedf'," +
		"`platform_id` varchar(64)  NULL DEFAULT NULL COMMENT '平台ID'," +
		"`subs_id` varchar(32)  NULL DEFAULT NULL COMMENT '用户id kafak导入'," +
		"`uuid` varchar(64)  NOT NULL DEFAULT '' COMMENT '唯一标识'," +
		"`create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'," +
		"`create_by` varchar(255)  NULL DEFAULT NULL COMMENT '创建者'," +
		"`update_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT '更新时间'," +
		"`update_by` varchar(255)  NULL DEFAULT NULL COMMENT '更新者'," +
		"`icciddfaf` varchar(32)  NOT NULL COMMENT 'ICCID'," +
		"`imsiopld` varchar(32)  NULL DEFAULT NULL COMMENT 'imsiopld kafak导入'," +
		"`msisdn123` varchar(32)  NULL DEFAULT NULL COMMENT 'kafak导入'," +
		"`carrier` int(0) NOT NULL COMMENT '1:中国移动，1:中国电信，3:中国联通'," +
		"`imei` varchar(32)  NULL DEFAULT NULL COMMENT 'IMEI'," +
		"`vinodk` varchar(32)  NULL DEFAULT NULL COMMENT '车辆VIN码'," +
		"`open_` datetime(3) NULL DEFAULT NULL COMMENT '日期'," +
		"`active` datetime(3) NULL DEFAULT NULL COMMENT '日期'," +
		"`network_type` varchar(8)  NULL DEFAULT NULL COMMENT '网络类型（字典项编码）'," +
		"`card_type` varchar(8)  NULL DEFAULT NULL COMMENT '卡片物理类型（字典项编码）'," +
		"`belong_place` varchar(32)  NULL DEFAULT NULL COMMENT '归属地'," +
		"`remark` varchar(255)  NULL DEFAULT NULL COMMENT '备注'," +
		"`status_time` datetime(0) NULL DEFAULT NULL COMMENT '卡号状态变更时间'," +
		"`vehicle_status` int(0) NULL DEFAULT NULL COMMENT '车辆状态'," +
		"`vehicle_out_factory_time` datetime(3) NULL DEFAULT NULL COMMENT '车辆出厂时间'," +
		"`status` int(0) NOT NULL DEFAULT 0 COMMENT ''," +
		"`stattu1` int(0) NULL DEFAULT NULL COMMENT ''," +
		"`account_id` varchar(32)  NULL DEFAULT NULL COMMENT '账户id'," +
		"`account_name` varchar(64)  NULL DEFAULT NULL COMMENT '账户名称'," +
		"`plat_type` varchar(8)  NULL DEFAULT NULL COMMENT '平台类型2:pb 3:ct'," +
		"`cust_id` varchar(32)  NULL DEFAULT NULL COMMENT '客户id kafak导入'," +
		"`cust_name` varchar(32)  NULL DEFAULT NULL COMMENT '客户名称'," +
		"`cust_type` varchar(32)  NULL DEFAULT NULL COMMENT '客户类型 一般为C客户类型'," +
		"`be_id` varchar(32)  NULL DEFAULT NULL COMMENT '省份编码'," +
		"`region_id` varchar(32)  NULL DEFAULT NULL COMMENT '归属地编码'," +
		"`group_id` varchar(128)  NULL DEFAULT NULL COMMENT '归属群组id'," +
		"`group_member_status` varchar(128)  NULL DEFAULT NULL COMMENT '归属群组中成员状态'," +
		"`data_usage` bigint(0) NULL DEFAULT NULL COMMENT '用量'," +
		"`cust` varchar(255)  NULL DEFAULT NULL COMMENT '编码'," +
		"`sync_time` datetime(0) NULL DEFAULT NULL COMMENT '通过kafka入库时，每次必须更新的字段，其他入口不用变动'," +
		"`source_create_time` datetime(0) NULL DEFAULT NULL COMMENT '源端数据创建时间'," +
		"`source_modify_time` datetime(0) NULL DEFAULT NULL COMMENT '源端数据修改时间'," +
		"`boss` int(0) NOT NULL COMMENT '运营商BOSS系统，1-移动CT，2-移动PB，3-电信DCP，4-电信M2M，5-联通jasper，6-联通CMP，7-中国电信5GCMP平台'," +
		"`card_status` varchar(20)  NULL DEFAULT NULL COMMENT '卡状态'," +
		"`device_num` varchar(64)  NULL DEFAULT NULL COMMENT '设备号'," +
		"PRIMARY KEY (`id`) ," +
		"UNIQUE INDEX `uk_ecbase_card_vc_icciddfaf`(`vccodedosedf`, `icciddfaf`)  COMMENT '20231021新增'," +
		"INDEX `ecbase_card_ecbase_company_id_fk`(`company_id`) ," +
		"INDEX `ecbase_card_subs_id`(`subs_id`) ," +
		"INDEX `idx_ecbase_card_vin`(`vinodk`) ," +
		"INDEX `ecbase_card_code_carrier_card_status_idx`(`code`, `carrier`) ," +
		"INDEX `ecbase_card_code_vehicle_status_idx`(`code`, `vehicle_status`) ," +
		"INDEX `idx_account_id`(`account_id`) ," +
		"INDEX `idx_ecbase_card_msisdn123`(`msisdn123`) ," +
		"INDEX `ecbase_card_code_icciddfaf_idx`(`code`, `icciddfaf`) ," +
		"INDEX `idx_vc`(`vccodedosedf`) ," +
		"INDEX `ecbase_card_icciddfaf`(`icciddfaf`)  COMMENT '2310.7新增'" +
		");"

	db := createWideMIndexesTable(createTableSql)
	prefix := "(company_id, code, vccodedosedf, platform_id, subs_id, uuid, create_time, create_by, update_time, update_by, icciddfaf, imsiopld, msisdn123, carrier, imei, vinodk, open_, active, network_type, card_type, belong_place, remark, status_time, vehicle_status, vehicle_out_factory_time, status, stattu1, account_name, plat_type, cust_id, cust_name, cust_type, be_id, region_id, group_id, group_member_status, data_usage, cust, sync_time, source_create_time, source_modify_time, boss, account_id, card_status, device_num) "
	tableName := "wide_indexes_table" + prefix
	InsertWorker(db, tableName, generateWideMIndexesValues)
}

func generateWideMIndexesValues(s, e int, offset int64) (string, time.Duration) {
	start := time.Now()
	var values []string
	for idx := s; idx < e; idx++ {
		accompanyId := int64(idx) + offset

		values = append(values,
			fmt.Sprintf("(%d, '334', '$unique', 'sadf', '1sdfsa', 'adf', '2024-01-19 10:14:48', '1', '2024-01-19 10:14:48', '1', '%s', '4fsadf', '1235657898', 1, 'sadfr354', 'sdf456', '2024-01-19 18:13:29.000', '2024-01-19 18:13:31.000', '1', '2', '1', NULL, '2024-01-19 18:13:41', 2, '2024-01-19 18:14:47.000', 0, 1, 'sd', '1', '234365f', 'dfgfsh', '2', '230', '123', '123', '123', 123, '123', '2024-01-19 18:14:13', '2024-01-19 18:14:15', '2024-01-19 18:14:17', 2, 12, '1', '123wqr')",
				accompanyId, strconv.Itoa(int(accompanyId))))
	}
	return strings.Join(values, ","), time.Since(start)
}
