
# shellcheck disable=SC2164
cd ../insert_benchmark

go test -run Test_Main -withPK 0 -terminals 1 -sessions 1 -withTXN 1 -timeout 1h