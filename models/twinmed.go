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

// itemnmbr	Item Class Code	vnditnum	itemdesc	itemtype	docdate	sopnumbe	qty_in_uofm	uofm	unitcost	ext cost	Contract Cost	Unit Rbt	Ext Rbt	Contract	custnmbr	Vizient ID	Premier ID	custname	shiptoname	address1	address2	city	state	zipcode	locncode
// 005-716	NUR-005	716	Skin Staple Remover Kit, Sterile   48/cs	Sales Inventory	2/7/2024	INV11975272	2	EA	 0.92 	 1.84 	 0.78 	 0.14 	 0.28 	R203B1	UMG14	J5GD	Not Premier	Allure of Mendota	Allure of Mendota	1201 1st Ave		Mendota	IL	61342-1815	CHI

type Twinmed struct {
	Itemnmbr      string
	ItemClassCode string
	Vnditnum      string
	Itemdesc      string
	Itemtype      string
	Docdate       time.Time
	Sopnumbe      string
	QtyInUofm     float64
	Uofm          string
	Unitcost      float64
	ExtCost       float64
	ContractCost  float64
	UnitRbt       float64
	ExtRbt        float64
	Contract      string
	Custnmbr      string
	VizientID     string
	PremierID     string
	Custname      string
	Shiptoname    string
	Address1      string
	Address2      string
	City          string
	State         string
	Zipcode       string
	Locncode      string
}

func (t *Twinmed) ReadIntoStruct(record []string) error {
	layout := "1/2/2006"
	// Parse each field with the correct type
	t.Itemnmbr = record[0]
	t.ItemClassCode = record[1]
	t.Vnditnum = record[2]
	t.Itemdesc = record[3]
	t.Itemtype = record[4]
	t.Docdate, _ = time.Parse(layout, record[5])
	t.Sopnumbe = record[6]
	t.QtyInUofm = helpers.ConvertStrToFloat(record[7])
	t.Uofm = record[8]
	t.Unitcost = helpers.ConvertStrToFloat(record[9])
	t.ExtCost = helpers.ConvertStrToFloat(record[10])
	t.ContractCost = helpers.ConvertStrToFloat(record[11])
	t.UnitRbt = helpers.ConvertStrToFloat(record[12])
	t.ExtRbt = helpers.ConvertStrToFloat(record[13])
	t.Contract = record[14]
	t.Custnmbr = record[15]
	t.VizientID = record[16]
	t.PremierID = record[17]
	t.Custname = record[18]
	t.Shiptoname = record[19]
	t.Address1 = record[20]
	t.Address2 = record[21]
	t.City = record[22]
	t.State = record[23]
	t.Zipcode = record[24]
	t.Locncode = record[25]

	return nil
}

func (t *Twinmed) IsValidForInclusion() bool {
	return t.Contract != ""
}

func (t *Twinmed) GetRebateAmount() float64 {
	return t.ExtRbt
}

func (t *Twinmed) GetEmptyModel() interface{} {
	return t
}

func (t *Twinmed) ToTracingWithEnrichment(fileDate time.Time, fileName string, db *mongo.Database) Tracing {
	var tracing Tracing

	tracing.Period = fmt.Sprintf("%s%s-TWINMED_%s", strings.ToUpper(fileDate.Month().String()), strconv.Itoa(fileDate.Year()), fileDate.Format("20060102"))
	tracing.InvoiceDate = t.Docdate
	tracing.InvoiceNbr = helpers.StringWhiteSpace(t.Sopnumbe)
	tracing.OrderNbr = helpers.StringWhiteSpace(t.Sopnumbe)
	tracing.Contract = helpers.StringWhiteSpace(t.Contract)
	tracing.Gpo, _ = helpers.SearchContractsCollectionsForGpo(db, helpers.StringWhiteSpace(t.Contract))
	tracing.Part = helpers.StringWhiteSpace(t.Vnditnum)
	tracing.ShipQty = t.QtyInUofm
	// Uom should be regex and select only [a-zA-Z] characters
	tracing.Uom = regexp.MustCompile("[^a-zA-Z]").ReplaceAllString(t.Uofm, "")
	// calculation
	tracing.ShipQtyAsCs = helpers.ConvertUOM(db, tracing.Part, tracing.ShipQty, tracing.Uom)
	tracing.Rebate = t.ExtRbt
	tracing.UnitRebate = t.UnitRbt
	tracing.Cost = t.ExtCost
	tracing.Name = helpers.StringWhiteSpace(t.Custname)
	tracing.Addr = strings.Trim(helpers.StringWhiteSpace(t.Address1)+" "+helpers.StringWhiteSpace(t.Address2), " ")
	tracing.City = helpers.StringWhiteSpace(t.City)
	tracing.State = helpers.StringWhiteSpace(t.State)
	tracing.Postal = helpers.StringWhiteSpace(t.Zipcode)

	if slices.Contains(constants.VALID_GPOS, tracing.Gpo) {
		switch tracing.Gpo {
		case "MEDASSETS":
			if t.VizientID != "" && t.VizientID != "Not Vizient" {
				tracing.License = helpers.StringWhiteSpace(t.VizientID)
			} else {
				match, err := helpers.SearchForMemberLicense(db, tracing.Gpo, tracing.Name, tracing.Addr, tracing.City, tracing.State)
				if err != nil {
					tracing.License = "0"
				} else {
					tracing.License = match.MemberID
				}
			}

		case "PREMIER":
			if t.PremierID != "" && t.PremierID != "Not Premier" {
				tracing.License = helpers.StringWhiteSpace(t.PremierID)
			} else {
				match, err := helpers.SearchForMemberLicense(db, tracing.Gpo, tracing.Name, tracing.Addr, tracing.City, tracing.State)
				if err != nil {
					tracing.License = "0"
				} else {
					tracing.License = match.MemberID
				}
			}
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
