package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"os"
	"strings"
	"yesteapea.com/lsm/tree"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	// Inputs:
	// * create a new sstable and do reads/writes
	//    ./main new <file_prefix>
	// * load an existing sstable and do reads
	//    /main load <path>
	mode := os.Args[1]
	filePath := os.Args[2]
	var table *tree.SSTable
	if mode == "new" {
		filePath = filePath + uuid.NewString() + ".dump"
		fmt.Println("Will flush the sstable to", filePath)
		_table := tree.EmptyWithDefaultConfig(filePath)
		table = &_table
		defer func() {
			fmt.Println("Flushing to disk at", filePath)
			err := table.ConvertToSegmentFile()
			if err.GetError() != nil {
				fmt.Println("Failed in ConvertToSegmentFile", err.Error())
			} else {
				fmt.Println("Flushed to disk at", filePath)
			}
		}()
	} else {
		var err error
		table, err = tree.NewFromFile(filePath)
		if err != nil {
			panic(err)
		}
	}
	for {
		fmt.Print("Input: ")
		line, _ := reader.ReadString('\n')
		line = strings.Trim(line, "\n ")
		if line == "exit" {
			break
		} else if parts := strings.Split(line, " "); parts[0] == "get" && len(parts) == 2 {
			value, err := table.Get(parts[1])
			if !err.Success() {
				fmt.Println("value =", value)
			} else {
				fmt.Println("Does not exist: err =", value)
			}
		} else if parts[0] == "put" && len(parts) == 3 {
			err := table.Put(parts[1], parts[2])
			if !err.Success() {
				fmt.Println("Put failed", err.Error())
			}
		} else if parts[0] == "delete" && len(parts) == 2 {
			err := table.Delete(parts[1])
			if !err.Success() {
				fmt.Println("Delete failed", err.Error())
			}
		} else if parts[0] == "flush" && len(parts) == 1 {
			err := table.ConvertToSegmentFile()
			if !err.Success() {
				fmt.Println("ConvertToSegmentFile failed", err.Error())
			}
		}
	}

}
