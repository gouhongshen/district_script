
# shellcheck disable=SC2164
cd ../insert_benchmark

# withTxn 100: 100 insert in one txn
# keepTbl 0: delete old tbl before test
# withPK 1: insert test on a table which has pk

go test -run Test_Main -withPK 0 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 0 -insSize 1000000 -timeout 1h