package helpers

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
)

type ZipCode struct {
	Id          string `json:"-"`
	Zip         string `json:"zip"`
	PrimaryCity string `json:"primary_city"`
	State       string `json:"state"`
}

var ZipCodes = make(map[string]ZipCode)

func init() {
	err := ReadZipCodesIntoMap()
	if err != nil {
		panic(err)
	}

}

func ReadZipCodesIntoMap() error {
	file, err := os.Open("./helpers/zip_codes.json")
	if err != nil {
		return err
	}
	defer file.Close()

	var zipCodes []ZipCode
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&zipCodes)
	if err != nil {
		return err
	}

	for _, v := range zipCodes {
		ZipCodes[v.Zip] = v
	}

	return nil
}

func SearchByCityState(city string, state string) string {
	cityRegex := regexp.MustCompile("(?i)" + city) // case-insensitive match

	// Iterate over the zip codes
	for _, v := range ZipCodes {
		// Check if city and state match
		if cityRegex.MatchString(v.PrimaryCity) && v.State == strings.ToUpper(state) {
			return v.Zip
		}
	}

	return ""
}
