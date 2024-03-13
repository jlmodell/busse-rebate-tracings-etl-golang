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

type TriAnim struct {
	VendorID        string
	VendorName      string
	VendorContract  string
	CustomerID      string
	CustomerName    string
	ShipToID        string
	Address1        string
	Address2        string
	City            string
	State           string
	Zip             string
	AMID            string
	Invoice         string
	DPCInvoice      string
	InvType         string
	InvDate         time.Time
	PeriodDT        string
	ItemID          string
	ItemDescription string
	VendorItemID    string
	UoM             string
	Whse            string
	Qty             float64
	RebatedCost     float64
	RebateAmt       float64
	TotalRebate     float64
	StdCost         float64
	PriceSource     string
}

func (t *TriAnim) ReadIntoStruct(record []string) error {
	layout := "1/2/2006"
	// Parse each field with the correct type
	t.VendorID = record[0]
	t.VendorName = record[1]
	t.VendorContract = record[2]
	t.CustomerID = record[3]
	t.CustomerName = record[4]
	t.ShipToID = record[5]
	t.Address1 = record[6]
	t.Address2 = record[7]
	t.City = record[8]
	t.State = record[9]
	t.Zip = record[10]
	t.AMID = record[11]
	t.Invoice = record[12]
	t.DPCInvoice = record[13]
	t.InvType = record[14]
	t.InvDate, _ = time.Parse(layout, regexp.MustCompile("d{1,2}//d{1,2}//d{4}").ReplaceAllString(record[15], ""))
	t.PeriodDT = record[16]
	t.ItemID = record[17]
	t.ItemDescription = record[18]
	t.VendorItemID = record[19]
	t.UoM = record[20]
	t.Whse = record[21]
	t.Qty = helpers.ConvertStrToFloat(record[22])
	t.RebatedCost = helpers.ConvertStrToFloat(record[23])
	t.RebateAmt = helpers.ConvertStrToFloat(record[24])
	t.TotalRebate = helpers.ConvertStrToFloat(record[25])
	t.StdCost = helpers.ConvertStrToFloat(record[26])
	t.PriceSource = record[27]

	fmt.Printf("TriAnim: %+v\n", t)

	return nil
}

func (t *TriAnim) IsValidForInclusion() bool {
	return t.VendorContract != ""
}

func (t *TriAnim) GetRebateAmount() float64 {
	return t.TotalRebate
}

func (t *TriAnim) GetEmptyModel() interface{} {
	return t
}

func (t *TriAnim) ToTracingWithEnrichment(fileDate time.Time, fileName string, db *mongo.Database) Tracing {
	var tracing Tracing

	tracing.Period = fmt.Sprintf("%s%s-TRIANIM_%s", strings.ToUpper(fileDate.Month().String()), strconv.Itoa(fileDate.Year()), fileDate.Format("20060102"))
	tracing.InvoiceDate = t.InvDate
	tracing.InvoiceNbr = helpers.StringWhiteSpace(t.Invoice)
	tracing.OrderNbr = helpers.StringWhiteSpace(t.Invoice)
	tracing.Contract = helpers.StringWhiteSpace(t.VendorContract)
	tracing.Gpo, _ = helpers.SearchContractsCollectionsForGpo(db, helpers.StringWhiteSpace(t.VendorContract))
	tracing.Part = helpers.StringWhiteSpace(t.VendorItemID)
	tracing.ShipQty = t.Qty
	// Uom should be regex and select only [a-zA-Z] characters
	tracing.Uom = regexp.MustCompile("[^a-zA-Z]").ReplaceAllString(t.UoM, "")
	// calculation
	tracing.ShipQtyAsCs = helpers.ConvertUOM(db, tracing.Part, tracing.ShipQty, tracing.Uom)
	tracing.Rebate = t.TotalRebate
	tracing.UnitRebate = t.RebateAmt
	tracing.Cost = t.Qty * (t.StdCost - t.RebatedCost)
	tracing.Name = helpers.StringWhiteSpace(t.CustomerName)
	tracing.Addr = strings.Trim(helpers.StringWhiteSpace(t.Address1)+helpers.StringWhiteSpace(t.Address2), " ")
	tracing.City = helpers.StringWhiteSpace(t.City)
	tracing.State = helpers.StringWhiteSpace(t.State)
	tracing.Postal = helpers.StringWhiteSpace(t.Zip)

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
