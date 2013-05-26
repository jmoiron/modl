#!/bin/sh

#set -e

export GORP_TEST_DSN="gorptest/gorptest/"
export GORP_TEST_DIALECT="mysql"
go test -bench . -benchmem

export GORP_TEST_DSN="user=$USER dbname=gorptest sslmode=disable"
export GORP_TEST_DIALECT="postgres"
go test -bench . -benchmem

export GORP_TEST_DSN="/tmp/gorptest.bin"
export GORP_TEST_DIALECT="sqlite"
go test  -bench . -benchmem

# PASS
# BenchmarkNativeCrud	    2000	    872061 ns/op	    5364 B/op	     106 allocs/op
# BenchmarkGorpCrud	    2000	    918331 ns/op	    7257 B/op	     159 allocs/op
# ok  	_/Users/jmoiron/dev/go/gorp	4.413s
# PASS
# BenchmarkNativeCrud	    1000	   1468223 ns/op	    8833 B/op	     335 allocs/op
# BenchmarkGorpCrud	    1000	   1485000 ns/op	   11585 B/op	     390 allocs/op
#  ok  	_/Users/jmoiron/dev/go/gorp	3.664s
# PASS
# BenchmarkNativeCrud	    1000	   1761324 ns/op	    1446 B/op	      51 allocs/op
# BenchmarkGorpCrud	    1000	   1733306 ns/op	    2942 B/op	      99 allocs/op
# ok  	_/Users/jmoiron/dev/go/gorp	3.977s
#

