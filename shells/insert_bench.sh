
# shellcheck disable=SC2164
cd ../insert_benchmark

# withTxn 100: 100 insert in one txn
# keepTbl 0: delete old tbl before test
# withPK 1: insert test on a table which has one pk
# -timeout 1h: set the golang test timeout
# insSize 1000: trying to insert 1000 rows

# go test -run Test_Statement_CU_Insert -terminals 20 -sessions 20 -withTXN 1025 -insSize 102500 -timeout 1h

#  go test -run Test_WideMIndexedTable_Insert -load 1 -insSize 40000000 -timeout 1h
# go test -run Test_WideMIndexedTable_Insert -terminals 150 -sessions 150 -withTXN 0 -keepTbl 1 -insSize 30000 -timeout 1h

########################################## no pk

# no pk - single insert - on empty table
# go test -run Test_Main -withPK 0 -terminals 1 -sessions 1 -withTXN 0 -keepTbl 0 -insSize 100000 -timeout 1h

# no pk - single insert - on 4000W rows table
# go test -run Test_Main -withPK 0 -terminals 40 -sessions 40 -withTXN 10000 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 0 -load 1 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 0 -terminals 1 -sessions 1 -withTXN 0 -keepTbl 0 -insSize 10 -timeout 1h

# no pk - batch insert - on empty table
# go test -run Test_Main -withPK 0 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 0 -insSize 1000000 -timeout 1h

# no pk - batch insert - on 4000W rows table
# go test -run Test_Main -withPK 0 -terminals 40 -sessions 40 -withTXN 10000 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 0 -keepTbl 0 -load 1 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 0 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 1 -insSize 1000000 -timeout 1h


########################################## one pk

# one pk - single insert - on empty table
 go test -run Test_Main -withPK 1 -terminals 5 -sessions 5 -withTXN 2000 -keepTbl 1 -insSize 10000000000 -timeout 20h

# one pk - single insert - on 4000W rows table
# go test -run Test_Main -withPK 1 -terminals 40 -sessions 40 -withTXN 8000 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 1 -load 1 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 1 -terminals 80 -sessions 80 -withTXN 0 -keepTbl 1 -insSize 200000 -timeout 1h

# one pk - batch insert - on empty table
# go test -run Test_Main -withPK 1 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 0 -insSize 1000000 -timeout 1h

# one pk - batch insert - on 4000W rows table
# go test -run Test_Main -withPK 1 -terminals 40 -sessions 40 -withTXN 10000 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 1 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 1 -insSize 300000 -timeout 1h


########################################## cluster pk

# cluster pk - single insert - on empty table
# go test -run Test_Main -withPK 2 -terminals 1 -sessions 1 -withTXN 0 -keepTbl 0 -insSize 100000 -timeout 1h

# cluster pk - single insert - on 4000W rows table
# go test -run Test_Main -withPK 2 -terminals 40 -sessions 40 -withTXN 10000 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 2 -terminals 1 -sessions 1 -withTXN 0 -keepTbl 1 -insSize 100000 -timeout 1h

# cluster pk - batch insert - on empty table
# go test -run Test_Main -withPK 2 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 0 -insSize 1000000 -timeout 1h

# cluster pk - batch insert - on 4000W rows table
# go test -run Test_Main -withPK 2 -terminals 40 -sessions 40 -withTXN 10000 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 2 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 1 -insSize 1000000 -timeout 1h
