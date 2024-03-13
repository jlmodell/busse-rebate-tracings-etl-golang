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

type Concordance struct {
	VendorNumber      string
	VendorName        string
	Warehouse         string
	BillToCustNumber  string
	BillToName        string
	BillToAddress1    string
	BillToAddress2    string
	BillToCity        string
	BillToState       string
	BillToZip         string
	ShipToNumber      string
	UniqueShipToID    string
	ShipToName        string
	ShipToAddress1    string
	ShipToAddress2    string
	ShipToCity        string
	ShipToState       string
	ShipToZip         string
	ShipToGPOID       string
	ShipToDEA         string
	InvoiceNumber     string
	InvoiceLineNumber string
	OrderDate         time.Time
	InvoiceDate       time.Time
	CHSItemNumber     string
	VendorItemNumber  string
	NDCNumber         string
	ItemDescription1  string
	ItemDescription2  string
	ItemDescription3  string
	UOMDescription    string
	UOM               string
	QtyShip           float64
	AcquisitionCost   float64
	ContractCost      float64
	Rebate            float64
	ExtendedRebate    float64
	ContractNumber    string
	ContractGroup     string
	DebitMemo         string
}

func (c *Concordance) ReadIntoStruct(record []string) error {
	// layout := "1/2/2006"
	layout := "20060102"
	// Parse each field with the correct type
	c.VendorNumber = record[0]
	c.VendorName = record[1]
	c.Warehouse = record[2]
	c.BillToCustNumber = record[3]
	c.BillToName = record[4]
	c.BillToAddress1 = record[5]
	c.BillToAddress2 = record[6]
	c.BillToCity = record[7]
	c.BillToState = record[8]
	c.BillToZip = record[9]
	c.ShipToNumber = record[10]
	c.UniqueShipToID = record[11]
	c.ShipToName = record[12]
	c.ShipToAddress1 = record[13]
	c.ShipToAddress2 = record[14]
	c.ShipToCity = record[15]
	c.ShipToState = record[16]
	c.ShipToZip = record[17]
	c.ShipToGPOID = record[18]
	c.ShipToDEA = record[19]
	c.InvoiceNumber = record[20]
	c.InvoiceLineNumber = record[21]
	c.OrderDate, _ = time.Parse(layout, strings.ReplaceAll(record[22], ".0", ""))
	c.InvoiceDate, _ = time.Parse(layout, strings.ReplaceAll(record[23], ".0", ""))
	c.CHSItemNumber = record[24]
	c.VendorItemNumber = record[25]
	c.NDCNumber = record[26]
	c.ItemDescription1 = record[27]
	c.ItemDescription2 = record[28]
	c.ItemDescription3 = record[29]
	c.UOMDescription = record[30]
	c.UOM = record[31]
	c.QtyShip = helpers.ConvertStrToFloat(record[32])
	c.AcquisitionCost = helpers.ConvertStrToFloat(record[33])
	c.ContractCost = helpers.ConvertStrToFloat(record[34])
	c.Rebate = helpers.ConvertStrToFloat(record[35])
	c.ExtendedRebate = helpers.ConvertStrToFloat(record[36])
	c.ContractNumber = record[37]
	c.ContractGroup = record[38]
	c.DebitMemo = record[39]

	return nil
}

func (c *Concordance) IsValidForInclusion() bool {
	return c.ContractNumber != ""
}

func (c *Concordance) GetRebateAmount() float64 {
	return c.ExtendedRebate
}

func (c *Concordance) GetEmptyModel() interface{} {
	return c
}

func (c *Concordance) ToTracingWithEnrichment(fileDate time.Time, db *mongo.Database) Tracing {
	var tracing Tracing

	tracing.Period = fmt.Sprintf("%s%s-CONCORDANCE_%s", strings.ToUpper(fileDate.Month().String()), strconv.Itoa(fileDate.Year()), fileDate.Format("20060102"))
	tracing.InvoiceDate = c.InvoiceDate
	tracing.InvoiceNbr = helpers.StringWhiteSpace(strings.ReplaceAll(c.InvoiceNumber, ".0", ""))
	tracing.OrderNbr = helpers.StringWhiteSpace(strings.ReplaceAll(c.InvoiceNumber, ".0", ""))
	tracing.Contract = helpers.StringWhiteSpace(strings.ReplaceAll(c.ContractNumber, ".0", ""))
	tracing.Gpo, _ = helpers.SearchContractsCollectionsForGpo(db, helpers.StringWhiteSpace(c.ContractNumber))
	tracing.Part = helpers.StringWhiteSpace(c.VendorItemNumber)
	tracing.ShipQty = c.QtyShip
	// Uom should be regex and select only [a-zA-Z] characters
	tracing.Uom = regexp.MustCompile("[^a-zA-Z]").ReplaceAllString(c.UOM, "")
	// calculation
	tracing.ShipQtyAsCs = helpers.ConvertUOM(db, tracing.Part, tracing.ShipQty, tracing.Uom)
	tracing.Rebate = c.ExtendedRebate
	tracing.Cost = c.ContractCost
	tracing.Name = helpers.StringWhiteSpace(c.ShipToName)
	tracing.Addr = strings.Trim(helpers.StringWhiteSpace(c.ShipToAddress1)+" "+helpers.StringWhiteSpace(c.ShipToAddress2), " ")
	tracing.City = helpers.StringWhiteSpace(c.ShipToCity)
	tracing.State = helpers.StringWhiteSpace(c.ShipToState)
	tracing.Postal = helpers.StringWhiteSpace(c.ShipToZip)

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
