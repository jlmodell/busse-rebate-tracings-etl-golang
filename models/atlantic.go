package models

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	constants "github.com/jlmodell/busse-rebate-tracings-etl-golang/constants"
	helpers "github.com/jlmodell/busse-rebate-tracings-etl-golang/helpers"
	"go.mongodb.org/mongo-driver/mongo"
)

type Atlantic struct {
	ContractNumber  string
	BussePartNumber string
	UOM             string
	ShipToName      string
	ItemDescription string
	InvoiceDate     time.Time
	InvoiceNumber   string
	QtyShip         float64
	AcquisitionCost float64
	ContractCost    float64
	ExtendedCost    float64
	ShipToAddress1  string
	ShipToAddress2  string
	ShipToCity      string
	ShipToState     string
	ShipToZip       string
}

func (a *Atlantic) ReadIntoStruct(record []string) error {
	layout := "01/02/2006"
	// layout := "20060102"
	// Parse each field with the correct type
	// Custom Parser for Part
	part := strings.ReplaceAll(strings.ReplaceAll(record[1], "BUS", ""), "-IMC", "")
	//
	a.ContractNumber = record[0]
	a.BussePartNumber = part
	a.UOM = record[2]
	a.ShipToName = record[3]
	a.ItemDescription = record[4]
	a.InvoiceDate, _ = time.Parse(layout, record[5])
	a.InvoiceNumber = record[6]
	a.QtyShip = helpers.ConvertStrToFloat(record[7])
	a.AcquisitionCost = helpers.ConvertStrToFloat(record[8])
	a.ContractCost = helpers.ConvertStrToFloat(record[9])
	a.ExtendedCost = helpers.ConvertStrToFloat(record[10])
	a.ShipToAddress1 = record[11]
	a.ShipToAddress2 = record[12]
	a.ShipToCity = record[13]
	a.ShipToState = record[14]
	a.ShipToZip = record[15]

	return nil
}

func (a *Atlantic) IsValidForInclusion() bool {
	return a.ContractNumber != ""
}

func (a *Atlantic) GetRebateAmount() float64 {
	return a.ExtendedCost
}

func (a *Atlantic) GetEmptyModel() interface{} {
	return a
}

func (a *Atlantic) ToTracingWithEnrichment(fileDate time.Time, fileName string, db *mongo.Database) Tracing {
	var tracing Tracing

	tracing.Period = fmt.Sprintf("%s%s-ATLANTICMED_%s", strings.ToUpper(fileDate.Month().String()), strconv.Itoa(fileDate.Year()), fileDate.Format("20060102"))
	tracing.InvoiceDate = a.InvoiceDate
	tracing.InvoiceNbr = helpers.StringWhiteSpace(a.InvoiceNumber)
	tracing.OrderNbr = helpers.StringWhiteSpace(a.InvoiceNumber)
	tracing.Contract = helpers.StringWhiteSpace(a.ContractNumber)
	tracing.Gpo, _ = helpers.SearchContractsCollectionsForGpo(db, helpers.StringWhiteSpace(a.ContractNumber))
	tracing.Part = helpers.StringWhiteSpace(strings.ReplaceAll(a.BussePartNumber, "BUS", ""))
	tracing.ShipQty = a.QtyShip
	// Uom should be regex and select only [a-zA-Z] characters
	tracing.Uom = regexp.MustCompile("[^a-zA-Z]").ReplaceAllString(a.UOM, "")
	// calculation
	tracing.ShipQtyAsCs = helpers.ConvertUOM(db, tracing.Part, tracing.ShipQty, tracing.Uom)
	tracing.Rebate = a.ExtendedCost
	tracing.UnitRebate = a.ExtendedCost / a.QtyShip
	tracing.Cost = a.ContractCost * a.QtyShip
	tracing.Name = helpers.StringWhiteSpace(a.ShipToName)
	tracing.Addr = strings.Trim(helpers.StringWhiteSpace(a.ShipToAddress1)+" "+helpers.StringWhiteSpace(a.ShipToAddress2), " ")
	tracing.City = helpers.StringWhiteSpace(a.ShipToCity)
	tracing.State = helpers.StringWhiteSpace(a.ShipToState)
	tracing.Postal = helpers.StringWhiteSpace(a.ShipToZip)

	if slices.Contains(constants.VALID_GPOS, tracing.Gpo) {
		match, err := helpers.SearchForMemberLicense(db, tracing.Gpo, tracing.Name, tracing.Addr, tracing.City, tracing.State)
		if err != nil {
			tracing.License = "0"
		} else {
			tracing.License = match.MemberID
		}
	} else {
		tracing.License = "0"
	}

	tracing.SearchScore = 0.0
	tracing.CheckLicense = false

	tracing.FileDate = fileDate
	tracing.FileName = fileName
	_, month, _ := fileDate.Date()
	tracing.PeriodMonth = fmt.Sprintf("%02d", month)
	tracing.PeriodYear = strconv.Itoa(fileDate.Year())

	fmt.Printf("Tracing: %+v\n", tracing)

	return tracing
}
