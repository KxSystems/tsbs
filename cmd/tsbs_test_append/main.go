package main

import (
	"bufio"
	"log"
	"os"
)

type fileDataSource struct {
	scanner *bufio.Scanner
}

type batch struct {
	rows [][]byte
}

func (b *batch) Len() uint {
	return uint(len(b.rows))
}

func (b *batch) Append(row []byte) {
	temp := make([]byte, len(row))
	copy(temp, row)
	b.rows = append(b.rows, temp)
	/*
		for index, row := range b.rows {
			log.Printf("row %d: %s\n", index, string(row))
		}
		if len(b.rows) > 43 {

			log.Fatalf("asdf")
		}
	*/
}

type factory struct{}

func (f *factory) New() *batch {
	return &batch{rows: make([][]byte, 0)}
}

func main() {
	file, _ := os.Open("/tmp/data1.csv")
	scanner := bufio.NewScanner(bufio.NewReaderSize(file, 4<<20))
	b := batch{}

	for scanner.Scan() {
		b.Append(scanner.Bytes())
	}
	for index, row := range b.rows {
		log.Printf("row %d: %s\n", index, string(row))
	}
}
