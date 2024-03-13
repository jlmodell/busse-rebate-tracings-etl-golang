package helpers

import (
	"context"
	"slices"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Contract struct {
	Contract string
	Gpo      string
}

var gpoCache = make(map[string]string)

func InGpoCache(contract string) (string, bool) {
	gpo, ok := gpoCache[contract]
	return gpo, ok
}

func SearchContractsCollectionsForGpo(db *mongo.Database, contract string) (string, error) {
	collection := db.Collection("contracts")

	trimmedContract := contract[:5] + ".*"

	if gpo, ok := InGpoCache(trimmedContract); ok {
		return gpo, nil
	}

	filter := bson.M{
		"contract": bson.M{
			"$regex":   trimmedContract,
			"$options": "i",
		},
	}

	var result Contract
	err := collection.FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		gpoCache[trimmedContract] = "Contract Not Found."
		return "Contract Not Found.", nil
	}

	if slices.Contains([]string{"VHA", "MEDASSETS", "VIZIENT"}, strings.ToUpper(result.Gpo)) {
		gpoCache[trimmedContract] = "MEDASSETS"
	} else {
		gpoCache[trimmedContract] = strings.ToUpper(result.Gpo)
	}

	return gpoCache[trimmedContract], nil
}
