package helpers

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/tealeg/xlsx"
)

func ConvertToCSV(fp string) (string, error) {
	xlsxFile, err := xlsx.OpenFile(fp)
	if err != nil {
		log.Printf("Error opening XLSX file: %s\n", err)
		return "", err
	}

	csvFilePath := strings.Replace(fp, ".xlsx", ".csv", -1)

	// Create or open the CSV file
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		fmt.Printf("Error creating CSV file: %s\n", err)
		return "", err
	}
	defer csvFile.Close()

	// Create a CSV writer
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	// Iterate through each sheet in the XLSX file
	for _, sheet := range xlsxFile.Sheets {
		for _, row := range sheet.Rows {
			// Iterate through each cell in the row and write to CSV
			var csvRow []string
			for _, cell := range row.Cells {
				csvRow = append(csvRow, cell.String())
			}
			csvWriter.Write(csvRow)
		}
	}

	return csvFilePath, nil
}
