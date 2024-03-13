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

type Henryschein struct {
	ContractNumber             string
	ExternalContract           string
	ContractDescription        string
	DebitMemoNumber            string
	Manf                       string
	CustomerSupplierItemNumber string
	ItemNumber                 string
	ItemDescription            string
	SizePkg                    string
	Strength                   string
	QuantityShipped            float64
	FileCost                   float64
	NetCost                    float64
	ChargebackPerUnitAmount    float64
	ExtendedChargebackAmount   float64
	GLDate                     time.Time
	OrderDate                  time.Time
	ShippedDate                time.Time
	InvoiceNumber              string
	OrderNumber                string
	GLNNumber                  string
	CustomerHINNumber          string
	CustomerDEANumber          string
	SoldToNumber               string
	SoldToName                 string
	SoldToBusinessName         string
	SoldToCareOfAttn           string
	SoldToAddressLine3         string
	SoldToAddressLine4         string
	SoldToCity                 string
	SoldToState                string
	SoldToPostalCode           string
	SoldToCountry              string
	BillToNumber               string
	BillToBusinessName         string
	BillToCareOfAttn           string
	BillToAddressLine3         string
	BillToAddressLine4         string
	BillToCity                 string
	BillToState                string
	BillToPostalCode           string
	BillToCountry              string
	VendorInvoiceNumber        string
	AdjNumberSchedule          string
	DivCode                    string
	MarketSegment              string
	PracticeType               string
	CustomerSubProfession      string
}

func (h *Henryschein) ReadIntoStruct(record []string) error {
	layout := "01-02-06"
	// Parse each field with the correct type
	h.ContractNumber = record[0]
	h.ExternalContract = record[1]
	h.ContractDescription = record[2]
	h.DebitMemoNumber = record[3]
	h.Manf = record[4]
	h.CustomerSupplierItemNumber = record[5]
	h.ItemNumber = record[6]
	h.ItemDescription = record[7]
	h.SizePkg = record[8]
	h.Strength = record[9]
	h.QuantityShipped, _ = strconv.ParseFloat(record[10], 64)
	h.FileCost, _ = strconv.ParseFloat(strings.TrimPrefix(record[11], "$"), 64)
	h.NetCost, _ = strconv.ParseFloat(strings.TrimPrefix(record[12], "$"), 64)
	h.ChargebackPerUnitAmount, _ = strconv.ParseFloat(strings.TrimPrefix(record[13], "$"), 64)
	h.ExtendedChargebackAmount, _ = strconv.ParseFloat(strings.TrimPrefix(record[14], "$"), 64)
	h.GLDate, _ = time.Parse(layout, record[15])
	h.OrderDate, _ = time.Parse(layout, record[16])
	h.ShippedDate, _ = time.Parse(layout, record[17])
	h.InvoiceNumber = record[18]
	h.OrderNumber = record[19]
	h.GLNNumber = record[20]
	h.CustomerHINNumber = record[21]
	h.CustomerDEANumber = record[22]
	h.SoldToNumber = record[23]
	h.SoldToName = record[24]
	h.SoldToBusinessName = record[25]
	h.SoldToCareOfAttn = record[26]
	h.SoldToAddressLine3 = record[27]
	h.SoldToAddressLine4 = record[28]
	h.SoldToCity = record[29]
	h.SoldToState = record[30]
	h.SoldToPostalCode = record[31]
	h.SoldToCountry = record[32]
	h.BillToNumber = record[33]
	h.BillToBusinessName = record[34]
	h.BillToCareOfAttn = record[35]
	h.BillToAddressLine3 = record[36]
	h.BillToAddressLine4 = record[37]
	h.BillToCity = record[38]
	h.BillToState = record[39]
	h.BillToPostalCode = record[40]
	h.BillToCountry = record[41]
	h.VendorInvoiceNumber = record[42]
	h.AdjNumberSchedule = record[43]
	h.DivCode = record[44]
	h.MarketSegment = record[45]
	h.PracticeType = record[46]
	h.CustomerSubProfession = record[47]

	return nil
}

func (h *Henryschein) IsValidForInclusion() bool {
	return h.ExternalContract != ""
}

func (h *Henryschein) GetRebateAmount() float64 {
	return h.ExtendedChargebackAmount
}

func (h *Henryschein) GetEmptyModel() interface{} {
	return h
}

func (h *Henryschein) ToTracingWithEnrichment(fileDate time.Time, db *mongo.Database) Tracing {
	var tracing Tracing

	tracing.Period = fmt.Sprintf("%s%s-HENRYSCHEIN_%s", strings.ToUpper(fileDate.Month().String()), strconv.Itoa(fileDate.Year()), fileDate.Format("20060102"))
	tracing.InvoiceDate = h.ShippedDate
	tracing.InvoiceNbr = helpers.StringWhiteSpace(h.InvoiceNumber)
	tracing.OrderNbr = helpers.StringWhiteSpace(h.OrderNumber)
	tracing.Contract = helpers.StringWhiteSpace(h.ExternalContract)
	tracing.Gpo, _ = helpers.SearchContractsCollectionsForGpo(db, helpers.StringWhiteSpace(h.ExternalContract))
	tracing.Part = helpers.StringWhiteSpace(h.CustomerSupplierItemNumber)
	tracing.ShipQty = h.QuantityShipped
	// Uom should be regex and select only [a-zA-Z] characters
	tracing.Uom = regexp.MustCompile("[^a-zA-Z]").ReplaceAllString(h.SizePkg, "")
	// calculation
	tracing.ShipQtyAsCs = helpers.ConvertUOM(db, tracing.Part, tracing.ShipQty, tracing.Uom)
	tracing.Rebate = h.ExtendedChargebackAmount
	tracing.UnitRebate = h.ExtendedChargebackAmount / h.QuantityShipped
	tracing.Cost = h.NetCost
	tracing.Name = helpers.StringWhiteSpace(h.SoldToName)
	tracing.Addr = strings.Trim(helpers.StringWhiteSpace(h.SoldToAddressLine3), " ")
	tracing.City = helpers.StringWhiteSpace(h.SoldToCity)
	tracing.State = helpers.StringWhiteSpace(h.SoldToState)
	tracing.Postal = helpers.StringWhiteSpace(h.SoldToPostalCode)

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
