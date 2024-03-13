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

type Dealmed struct {
	CustomerID              string
	EndUserName             string
	EndUserAddress          string
	EndUserCity             string
	EndUserState            string
	EndUserZip              string
	InvoiceNumber           string
	InvoiceDate             time.Time
	BusseItemNumber         string
	QuantitySold            float64
	UOM                     string
	PurchasePrice           float64
	ExtendedPurchasePrice   float64
	ContractPrice           float64
	ExtendedContractPrice   float64
	UnitRebateRequested     float64
	ExtendedRebateRequested float64
	Contract                string
}

func (d *Dealmed) ReadIntoStruct(record []string) error {
	layout := "1/2/2006"
	// Parse each field with the correct type
	d.CustomerID = record[0]
	d.EndUserName = record[1]
	d.EndUserAddress = record[2]
	d.EndUserCity = record[3]
	d.EndUserState = record[4]
	d.EndUserZip = record[5]
	d.InvoiceNumber = record[6]
	d.InvoiceDate, _ = time.Parse(layout, record[7])
	d.BusseItemNumber = record[8]
	d.QuantitySold = helpers.ConvertStrToFloat(record[9])
	d.UOM = record[10]
	d.PurchasePrice = helpers.ConvertStrToFloat(record[11])
	d.ExtendedPurchasePrice = helpers.ConvertStrToFloat(record[12])
	d.ContractPrice = helpers.ConvertStrToFloat(record[13])
	d.ExtendedContractPrice = helpers.ConvertStrToFloat(record[14])
	d.UnitRebateRequested = helpers.ConvertStrToFloat(record[15])
	d.ExtendedRebateRequested = helpers.ConvertStrToFloat(record[16])
	d.Contract = record[17]

	return nil
}

func (d *Dealmed) IsValidForInclusion() bool {
	return d.Contract != ""
}

func (d *Dealmed) GetRebateAmount() float64 {
	return d.ExtendedRebateRequested
}

func (d *Dealmed) GetEmptyModel() interface{} {
	return d
}

func (d *Dealmed) ToTracingWithEnrichment(fileDate time.Time, db *mongo.Database) Tracing {
	var tracing Tracing

	tracing.Period = fmt.Sprintf("%s%s-DEALMED_%s", strings.ToUpper(fileDate.Month().String()), strconv.Itoa(fileDate.Year()), fileDate.Format("20060102"))
	tracing.InvoiceDate = d.InvoiceDate
	tracing.InvoiceNbr = helpers.StringWhiteSpace(d.InvoiceNumber)
	tracing.OrderNbr = helpers.StringWhiteSpace(d.InvoiceNumber)
	tracing.Contract = helpers.StringWhiteSpace(d.Contract)
	tracing.Gpo, _ = helpers.SearchContractsCollectionsForGpo(db, helpers.StringWhiteSpace(d.Contract))
	tracing.Part = helpers.StringWhiteSpace(d.BusseItemNumber)
	tracing.ShipQty = d.QuantitySold
	// Uom should be regex and select only [a-zA-Z] characters
	tracing.Uom = regexp.MustCompile("[^a-zA-Z]").ReplaceAllString(d.UOM, "")
	// calculation
	tracing.ShipQtyAsCs = helpers.ConvertUOM(db, tracing.Part, tracing.ShipQty, tracing.Uom)
	tracing.Rebate = d.ExtendedRebateRequested
	tracing.UnitRebate = d.UnitRebateRequested
	tracing.Cost = d.ExtendedContractPrice
	tracing.Name = helpers.StringWhiteSpace(d.EndUserName)
	tracing.Addr = strings.Trim(helpers.StringWhiteSpace(d.EndUserAddress), " ")
	tracing.City = helpers.StringWhiteSpace(d.EndUserCity)
	tracing.State = helpers.StringWhiteSpace(d.EndUserState)
	tracing.Postal = helpers.StringWhiteSpace(d.EndUserZip)

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

	fmt.Printf("Tracing: %+v\n", tracing)

	return tracing
}
