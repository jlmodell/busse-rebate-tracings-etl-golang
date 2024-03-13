package models

import (
	"time"
)

type Tracing struct {
	Period       string    `bson:"period"`
	InvoiceDate  time.Time `bson:"invoice_date"`
	InvoiceNbr   string    `bson:"invoice_nbr"`
	ClaimNbr     string    `bson:"claim_nbr"`
	OrderNbr     string    `bson:"order_nbr"`
	Contract     string    `bson:"contract"`
	Gpo          string    `bson:"gpo"`
	License      string    `bson:"license"`
	CheckLicense bool      `bson:"check_license"`
	SearchScore  float64   `bson:"searchScore"`
	Part         string    `bson:"part"`
	ShipQty      float64   `bson:"ship_qty"`
	Uom          string    `bson:"uom"`
	ShipQtyAsCs  float64   `bson:"ship_qty_as_cs"`
	Rebate       float64   `bson:"rebate"`
	UnitRebate   float64   `bson:"unit_rebate"`
	Cost         float64   `bson:"cost"`
	Name         string    `bson:"name"`
	Addr         string    `bson:"addr"`
	City         string    `bson:"city"`
	State        string    `bson:"state"`
	Postal       string    `bson:"postal"`
	FileDate     time.Time `bson:"__date__"`
	FileName     string    `bson:"__file__"`
	PeriodMonth  string    `bson:"__month__"`
	PeriodYear   string    `bson:"__year__"`
}

func (t *Tracing) GetKey() string {
	return t.Period
}

func (t *Tracing) New(data []interface{}) error {
	var err error
	t.Period = data[0].(string)
	t.InvoiceDate, err = time.Parse("2006-01-02", data[1].(string))
	if err != nil {
		return err
	}
	t.InvoiceNbr = data[2].(string)
	t.ClaimNbr = data[3].(string)
	t.OrderNbr = data[4].(string)
	t.Contract = data[5].(string)
	t.Gpo = data[6].(string)
	t.License = data[7].(string)
	t.CheckLicense = data[8].(bool)
	t.SearchScore = data[9].(float64)
	t.Part = data[10].(string)
	t.ShipQty = data[11].(float64)
	t.Uom = data[12].(string)
	t.ShipQtyAsCs = data[13].(float64)
	t.Rebate = data[14].(float64)
	t.UnitRebate = data[15].(float64)
	t.Cost = data[16].(float64)
	t.Name = data[17].(string)
	t.Addr = data[18].(string)
	t.City = data[19].(string)
	t.State = data[20].(string)
	t.Postal = data[21].(string)

	return nil
}
