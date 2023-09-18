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
	var table tree.KVStore
	if mode == "new" {
		filePath = filePath + uuid.NewString() + ".dump"
		fmt.Println("Will flush the sstable to", filePath)
		lsmTable := tree.Empty(filePath)
		table = lsmTable
		defer func() {
			fmt.Println("Flushing to disk at", filePath)
			err := lsmTable.FlushToDisk()
			if err.GetError() != nil {
				fmt.Println("Failed in FlushToDisk", err.Error())
			} else {
				fmt.Println("Flushed to disk at", filePath)
			}
		}()
	} else if mode == "load_lsm" {
		var err tree.DbError
		table, err = tree.LoadLsmTree(filePath)
		if !err.Success() {
			panic(err)
		}
	} else if mode == "load_sst" {
		var err error
		table, err = tree.NewSsTableFromFile(filePath)
		if err != nil {
			panic(err)
		}
	} else {
		fmt.Println("Invalid mode", mode)
		os.Exit(1)
	}

	for {
		fmt.Print("Input: ")
		line, _ := reader.ReadString('\n')
		line = strings.Trim(line, "\n ")
		if line == "exit" {
			break
		} else if parts := strings.Split(line, " "); parts[0] == "get" && len(parts) == 2 {
			valueBytes, err := table.Get([]byte(parts[1]))
			if !err.Success() {
				fmt.Println("Error =", err.Error())
			} else {
				value := string(valueBytes)
				fmt.Println("Value", value)
			}
		} else if parts[0] == "put" && len(parts) == 3 {
			err := table.Put([]byte(parts[1]), []byte(parts[2]))
			if !err.Success() {
				fmt.Println("Put failed", err.Error())
			}
		} else if parts[0] == "delete" && len(parts) == 2 {
			err := table.Delete([]byte(parts[1]))
			if !err.Success() {
				fmt.Println("Delete failed", err.Error())
			}
		} else if parts[0] == "flush" && len(parts) == 1 {
			if lsm, ok := table.(*tree.LSMTree); ok {
				var err tree.DbError
				err = lsm.FlushToDisk()
				if !err.Success() {
					fmt.Println("flush failed", err.Error())
				}
			} else {
				fmt.Println("flush supported only on lsm")
			}
		}
	}

}
