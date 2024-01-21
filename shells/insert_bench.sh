
# shellcheck disable=SC2164
cd ../insert_benchmark

# withTxn 100: 100 insert in one txn
# keepTbl 0: delete old tbl before test
# withPK 1: insert test on a table which has one pk
# -timeout 1h: set the golang test timeout
# insSize 1000: trying to insert 1000 rows

go test -run Test_Main -withPK 1 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 1 -insSize 300000 -timeout 1h
