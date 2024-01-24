package shells


# shellcheck disable=SC2164
cd ../insert_benchmark


########################################## no pk

# no pk - single insert - on empty table
# go test -run Test_Main -withPK 0 -terminals 1 -sessions 1 -withTXN 0 -keepTbl 0 -insSize 100000 -timeout 1h

# no pk - single insert - on 4000W rows table
 go test -run Test_Main -withPK 0 -terminals 40 -sessions 40 -withTXN 0 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 0 -terminals 40 -sessions 40 -withTXN 0 -keepTbl 1 -insSize 1000000 -timeout 1h

# no pk - batch insert - on empty table
# go test -run Test_Main -withPK 0 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 0 -insSize 1000000 -timeout 1h

# no pk - batch insert - on 4000W rows table
# go test -run Test_Main -withPK 0 -terminals 40 -sessions 40 -withTXN 6000 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 0 -terminals 40 -sessions 40 -withTXN 100 -keepTbl 1 -insSize 1000000 -timeout 1h


########################################## one pk

# one pk - single insert - on empty table
# go test -run Test_Main -withPK 1 -terminals 1 -sessions 1 -withTXN 0 -keepTbl 0 -insSize 100000 -timeout 1h

# one pk - single insert - on 4000W rows table
# go test -run Test_Main -withPK 1 -terminals 40 -sessions 40 -withTXN 0 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 1 -terminals 40 -sessions 40 -withTXN 0 -keepTbl 1 -insSize 1000000 -timeout 1h

# one pk - batch insert - on empty table
# go test -run Test_Main -withPK 1 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 0 -insSize 1000000 -timeout 1h

# one pk - batch insert - on 4000W rows table
# go test -run Test_Main -withPK 1 -terminals 40 -sessions 40 -withTXN 6000 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 1 -terminals 40 -sessions 40 -withTXN 100 -keepTbl 1 -insSize 1000000 -timeout 1h


########################################## cluster pk

# cluster pk - single insert - on empty table
# go test -run Test_Main -withPK 2 -terminals 1 -sessions 1 -withTXN 0 -keepTbl 0 -insSize 100000 -timeout 1h

# cluster pk - single insert - on 4000W rows table
# go test -run Test_Main -withPK 2 -terminals 40 -sessions 40 -withTXN 0 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 2 -terminals 40 -sessions 40 -withTXN 0 -keepTbl 1 -insSize 1000000 -timeout 1h

# cluster pk - batch insert - on empty table
# go test -run Test_Main -withPK 2 -terminals 1 -sessions 1 -withTXN 100 -keepTbl 0 -insSize 1000000 -timeout 1h

# cluster pk - batch insert - on 4000W rows table
# go test -run Test_Main -withPK 2 -terminals 40 -sessions 40 -withTXN 6000 -keepTbl 0 -insSize 40000000 -timeout 1h
# go test -run Test_Main -withPK 2 -terminals 40 -sessions 40 -withTXN 100 -keepTbl 1 -insSize 1000000 -timeout 1h

