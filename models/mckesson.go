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

// Supplier Number	McK Vendor Abbreviation	Debit Memo Number	McK Serv Center	Mck Invoice Number	Order Date	Invoice Date	Process Date	Debit Memo Date	Vendor Contract #	Vendor Cat Number	NDC	Unit of Measure	Mck Cost	Contract Cost	Unit Chargeback	Qty Shipped	Ext Chargeback Amt Claimed	McK Bill Acct Number	Customer Name	Customer Address	Customer Address 2	Customer City	Customer State	Customer Zip Code	DEA License Number	HIN Number	340B Identifier	Business Unit	Unique Line Sequence Number	Inv Line Nbr	Original Invoice Number	Original Invoice Date	Shipping DC	JDE 867 Debit Memo
// 11718	BUSSE	2400226270	0100000	64637761	03/04/2024	03/04/2024	03/07/2024	03/09/2024	R203B1	749		EA	7.82	7.64	0.18	10.000	1.80	59934909	URGENT CARE GROUP, LLC	216 CENTERVIEW DR STE 100		BRENTWOOD	TN	37027-3226				PC	4019844182000120	12000	00000000		10	27459809

type Mckesson struct {
	SupplierNumber           string
	McKVendorAbbreviation    string
	DebitMemoNumber          string
	McKServCenter            string
	MckInvoiceNumber         string
	OrderDate                time.Time
	InvoiceDate              time.Time
	ProcessDate              time.Time
	DebitMemoDate            time.Time
	VendorContract           string
	VendorCatNumber          string
	NDC                      string
	UnitOfMeasure            string
	MckCost                  float64
	ContractCost             float64
	UnitChargeback           float64
	QtyShipped               float64
	ExtChargebackAmtClaimed  float64
	McKBillAcctNumber        string
	CustomerName             string
	CustomerAddress          string
	CustomerAddress2         string
	CustomerCity             string
	CustomerState            string
	CustomerZipCode          string
	DEALicenseNumber         string
	HINNumber                string
	Identifier340B           string
	BusinessUnit             string
	UniqueLineSequenceNumber string
	InvLineNbr               string
	OriginalInvoiceNumber    string
	OriginalInvoiceDate      time.Time
	ShippingDC               string
	JDE867DebitMemo          string
}

func (m *Mckesson) ReadIntoStruct(record []string) error {
	layout := "1/2/2006"
	// Parse each field with the correct type
	m.SupplierNumber = record[0]
	m.McKVendorAbbreviation = record[1]
	m.DebitMemoNumber = record[2]
	m.McKServCenter = record[3]
	m.MckInvoiceNumber = record[4]
	m.OrderDate, _ = time.Parse(layout, record[5])
	m.InvoiceDate, _ = time.Parse(layout, record[6])
	m.ProcessDate, _ = time.Parse(layout, record[7])
	m.DebitMemoDate, _ = time.Parse(layout, record[8])
	m.VendorContract = record[9]
	m.VendorCatNumber = record[10]
	m.NDC = record[11]
	m.UnitOfMeasure = record[12]
	m.MckCost = helpers.ConvertStrToFloat(record[13])
	m.ContractCost = helpers.ConvertStrToFloat(record[14])
	m.UnitChargeback = helpers.ConvertStrToFloat(record[15])
	m.QtyShipped = helpers.ConvertStrToFloat(record[16])
	m.ExtChargebackAmtClaimed = helpers.ConvertStrToFloat(record[17])
	m.McKBillAcctNumber = record[18]
	m.CustomerName = record[19]
	m.CustomerAddress = record[20]
	m.CustomerAddress2 = record[21]
	m.CustomerCity = record[22]
	m.CustomerState = record[23]
	m.CustomerZipCode = record[24]
	m.DEALicenseNumber = record[25]
	m.HINNumber = record[26]
	m.Identifier340B = record[27]
	m.BusinessUnit = record[28]
	m.UniqueLineSequenceNumber = record[29]
	m.InvLineNbr = record[30]
	m.OriginalInvoiceNumber = record[31]
	m.OriginalInvoiceDate, _ = time.Parse(layout, record[32])
	m.ShippingDC = record[33]
	m.JDE867DebitMemo = record[34]

	return nil
}

func (m *Mckesson) IsValidForInclusion() bool {
	return m.VendorContract != ""
}

func (m *Mckesson) GetRebateAmount() float64 {
	return m.ExtChargebackAmtClaimed
}

func (m *Mckesson) GetEmptyModel() interface{} {
	return m
}

func (m *Mckesson) ToTracingWithEnrichment(fileDate time.Time, db *mongo.Database) Tracing {
	var tracing Tracing

	tracing.Period = fmt.Sprintf("%s%s-MCKESSON_%s", strings.ToUpper(fileDate.Month().String()), strconv.Itoa(fileDate.Year()), fileDate.Format("20060102"))
	tracing.InvoiceDate = m.InvoiceDate
	tracing.InvoiceNbr = helpers.StringWhiteSpace(m.MckInvoiceNumber)
	tracing.OrderNbr = helpers.StringWhiteSpace(m.MckInvoiceNumber)
	tracing.Contract = helpers.StringWhiteSpace(m.VendorContract)
	tracing.Gpo, _ = helpers.SearchContractsCollectionsForGpo(db, helpers.StringWhiteSpace(m.VendorContract))
	tracing.Part = helpers.StringWhiteSpace(m.VendorCatNumber)
	tracing.ShipQty = m.QtyShipped
	// Uom should be regex and select only [a-zA-Z] characters
	tracing.Uom = regexp.MustCompile("[^a-zA-Z]").ReplaceAllString(m.UnitOfMeasure, "")
	// calculation
	tracing.ShipQtyAsCs = helpers.ConvertUOM(db, tracing.Part, tracing.ShipQty, tracing.Uom)
	tracing.Rebate = m.ExtChargebackAmtClaimed
	tracing.UnitRebate = m.UnitChargeback
	tracing.Cost = m.ContractCost
	tracing.Name = helpers.StringWhiteSpace(m.CustomerName)
	tracing.Addr = strings.Trim(helpers.StringWhiteSpace(m.CustomerAddress)+" "+helpers.StringWhiteSpace(m.CustomerAddress2), " ")
	tracing.City = helpers.StringWhiteSpace(m.CustomerCity)
	tracing.State = helpers.StringWhiteSpace(m.CustomerState)
	tracing.Postal = helpers.StringWhiteSpace(m.CustomerZipCode)

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
