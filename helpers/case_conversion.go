package helpers

import (
	"context"
	"regexp"
	"slices"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	EACH_ALIAS = []string{"EA", "PR", "TR", "PK", "EACH", "Ea", "Pr", "Tr", "Pk", "Each", "KIT", "Kit"}
	BOX_ALIAS  = []string{"BOX", "BX", "Box", "Bx", "BG", "Bg", "Bag", "BAG"}
	CASE_ALIAS = []string{"CS", "cs", "CA", "ca", "Ca", "Cs", "CASE", "Case", "CT", "ct", "Ct"}
)

type ItemDetail struct {
	Part         string  `bson:"part"`
	Description  string  `bson:"description"`
	EachPerCase  float64 `bson:"each_per_case"`
	BoxesPerCase float64 `bson:"num_of_dispenser_boxes_per_case"`
}

var itemCache = make(map[string]ItemDetail)

func InItemCache(item string) (ItemDetail, bool) {
	itemDetail, ok := itemCache[item]
	return itemDetail, ok
}

func FindItemInDatabase(db *mongo.Database, item string) (ItemDetail, error) {
	collection := db.Collection("sched_data")

	if itemDetail, ok := InItemCache(item); ok {
		return itemDetail, nil
	}

	filter := bson.M{
		"part": item,
	}

	var result ItemDetail
	err := collection.FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		itemCache[item] = ItemDetail{}
		return ItemDetail{}, err
	}

	itemCache[item] = result

	return result, nil
}

func ConvertUOM(db *mongo.Database, item string, shipQty float64, uom string) float64 {
	iDetail, _ := FindItemInDatabase(db, item)

	if slices.Contains([]string{"139", "153", "283", "284", "7190"}, item) && slices.Contains(BOX_ALIAS, uom) {
		return float64(shipQty)
	}

	re := regexp.MustCompile(`^pk`) // Match the string starting with 'pk'
	if item == "164" && re.MatchString(uom) {
		if iDetail.BoxesPerCase != 0 {
			return shipQty / iDetail.BoxesPerCase
		}
		return 0.0
	}

	if item == "770" && re.MatchString(uom) {
		shipQty *= 12
		eaches := 48
		return shipQty / float64(eaches)
	}

	if item == "3220" && slices.Contains(CASE_ALIAS, uom) {
		if iDetail.BoxesPerCase != 0 {
			return shipQty / iDetail.BoxesPerCase
		}
		return 0.0
	}

	if item == "795" && re.MatchString(uom) {
		if iDetail.EachPerCase != 0 {
			return shipQty / iDetail.EachPerCase
		}
		return 0.0
	}

	// Default case
	eaches := iDetail.EachPerCase
	boxes := iDetail.BoxesPerCase
	switch {
	case slices.Contains(EACH_ALIAS, uom):
		if eaches != 0 {
			return float64(shipQty) / float64(eaches)
		}
	case slices.Contains(BOX_ALIAS, uom):
		if boxes != 0 {
			return float64(shipQty) / float64(boxes)
		} else if eaches != 0 {
			return float64(shipQty) / float64(eaches)
		}
	case slices.Contains(CASE_ALIAS, uom):
		return float64(shipQty)
	}

	return 0.0
}
