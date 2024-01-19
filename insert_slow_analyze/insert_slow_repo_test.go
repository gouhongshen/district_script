package insert_slow_analyze

import (
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"testing"
)

/*
+--------------+------------------+------+------+---------+-------+--------------------------------+
| Field        | Type             | Null | Key  | Default | Extra | Comment                        |
+--------------+------------------+------+------+---------+-------+--------------------------------+
| statement_id | VARCHAR(36)      | NO   | PRI  | NULL    |       |                                |
| account      | VARCHAR(300)     | NO   |      | NULL    |       |                                |
| response_at  | DATETIME(3)      | YES  |      | NULL    |       |                                |
| cu           | DECIMAL128(23)   | NO   |      | NULL    |       |                                |
| account_id   | INT UNSIGNED(32) | NO   | PRI  | NULL    |       | the account_id added by the mo |
+--------------+------------------+------+------+---------+-------+--------------------------------+
*/
func Test_Select_And_Or(t *testing.T) {
	stmt, _ := os.Create("select_and_or.sql")
	stmt.WriteString("select sum(char_length(statement_id)),sum(char_length(account)),sum(char_length(response_at)),sum(cu),sum(account_id) from mo_catalog.statement_cu where ")

	// (statement_id=%s and account_id=%d) or
	ll := 200
	for i := 0; i < ll; i++ {
		stmt.WriteString(fmt.Sprintf("(statement_id='%s' and account_id=%d)", uuid.New().String(), rand.Int()))
		if i == ll-1 {
			stmt.WriteString(";")
		} else {
			stmt.WriteString(" or ")
		}
	}
	stmt.Sync()
	stmt.Close()
}

/*

select statement_id, account_id
from select statement

select statement_id, account_id where statement_id in ()...

*/

func Test_Select_In(t *testing.T) {
	stmt, _ := os.Create("select_in.sql")
	stmt.WriteString("select sum(char_length(statement_id)),sum(char_length(account)),sum(char_length(response_at)),sum(cu),sum(account_id) from mo_catalog.statement_cu where ")

	stmt.WriteString(fmt.Sprintf("statement_id in ("))
	ll := 200
	for i := 0; i < ll; i++ {
		stmt.WriteString(fmt.Sprintf("'%s'", uuid.New().String()))
		if i != ll-1 {
			stmt.WriteString(",")
		}
	}

	stmt.WriteString(") and account_id in (")
	for i := 0; i < ll; i++ {
		stmt.WriteString(fmt.Sprintf("%d", rand.Int()))
		if i != ll-1 {
			stmt.WriteString(",")
		}
	}

	stmt.WriteString(");")

	stmt.Sync()
	stmt.Close()

}

func Test_Insert(t *testing.T) {
	stmt, _ := os.Create("insert.sql")
	stmt.WriteString("begin;\n")

	batCnt := 100
	for x := 0; x < batCnt; x++ {
		batSize := 1025
		vals := ""
		for y := 0; y < batSize; y++ {
			stmtId := uuid.New().String()
			account := "account"
			responseAt := "2024-01-17 06:23:34.861392517"
			cu := 0
			accountId := x*batSize + y
			vals += fmt.Sprintf("('%s','%s','%s',%d,%d)", stmtId, account, responseAt, cu, accountId)

			if y != batSize-1 {
				vals += ","
			}
		}
		stmt.WriteString(fmt.Sprintf("insert into mo_catalog.statement_cu_for_test values %s;\n", vals))
	}

	stmt.WriteString("commit;\n")

	stmt.Sync()
	stmt.Close()
}
