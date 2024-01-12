
# shellcheck disable=SC2164
cd ../insert_benchmark

# withTxn 100: 100 insert in one txn
# keepTbl 0: delete old tbl before test
# withPK 1: insert test on a table which has pk

go test -run Test_Main -withPK 2 -terminals 2 -sessions 2 -withTXN 100 -keepTbl 0 -insSize 100000 -timeout 1h