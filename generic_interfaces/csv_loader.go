package instance

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"
)

type CSVLoader interface {
	ReadIntoStruct([]string) error
	IsValidForInclusion() bool
}

func ReadCSVStruct[T CSVLoader](fp string, prototype T) ([]T, error) {
	// Open the CSV file
	file, err := os.Open(fp)
	if err != nil {
		fmt.Printf("Error opening CSV file: %s\n", err)
		return nil, err
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read the headers from the first line
	_, err = reader.Read()
	if err != nil {
		fmt.Printf("Error reading CSV headers: %s\n", err)
		return nil, err
	}

	var lines []T

	// Read and process remaining lines
	for {
		// Read a line
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading CSV record: %s\n", err)
			return nil, err
		}

		// Use reflection to create a new instance of the prototype
		instance := reflect.New(reflect.TypeOf(prototype).Elem()).Interface().(T)

		// Populate the instance from the CSV record
		if err := instance.ReadIntoStruct(record); err != nil {
			fmt.Printf("Error loading CSV into struct: %s\n", err)
			return nil, err
		}

		if instance.IsValidForInclusion() {
			lines = append(lines, instance)
		}
	}

	return lines, nil
}
