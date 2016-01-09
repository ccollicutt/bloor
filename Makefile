.PHONY: run

default: run

run:
	go run bloor.go smoketest.go -V -z 10
