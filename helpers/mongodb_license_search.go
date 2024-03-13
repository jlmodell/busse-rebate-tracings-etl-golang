package helpers

import (
	"context"
	"regexp"

	constants "github.com/jlmodell/busse-rebate-tracings-etl-golang/constants"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	vizientOrMedassetsOrVha = regexp.MustCompile(`^(VIZ|MEDASSET|VHA)`)
)

func RecastGPO(gpo string) string {
	if vizientOrMedassetsOrVha.MatchString(gpo) {
		return "MEDASSETS"
	}
	return gpo
}

func BuildShouldQuery(name, addr, city, state string) []bson.M {
	should := make([]bson.M, 0)

	if name != "" {
		should = append(should, bson.M{
			"text": bson.M{
				"query": name,
				"path":  "alias",
				"score": bson.M{
					"boost": bson.M{"value": 8},
				},
			},
		})
	}

	if addr != "" {
		should = append(should, bson.M{
			"text": bson.M{
				"query": addr,
				"path":  "address",
				"score": bson.M{
					"boost": bson.M{"value": 5},
				},
			},
		})
	}

	if city != "" {
		should = append(should, bson.M{
			"text": bson.M{
				"query": city,
				"path":  "city",
				"score": bson.M{
					"boost": bson.M{"value": 4},
				},
			},
		})
	}

	if state != "" {
		should = append(should, bson.M{
			"text": bson.M{
				"query": state,
				"path":  "state",
				"score": bson.M{
					"boost": bson.M{"value": 2},
				},
			},
		})
	}

	return should
}

type Roster struct {
	MemberID    string  `bson:"member_id"`
	SearchScore float64 `bson:"searchScore"`
}

func BuildAggregationQuery(should []bson.M, gpo string) []bson.M {
	aggregation := make([]bson.M, 0)

	if len(should) > 0 {
		aggregation = append(aggregation, bson.M{
			"$search": bson.M{
				"index": constants.ATLAS_SEARCH_INDEX_NAME,
				"compound": bson.M{
					"should": should,
				},
			},
		})
	}

	if gpo != "" {
		aggregation = append(aggregation, bson.M{
			"$match": bson.M{
				"group_name": bson.M{"$regex": gpo, "$options": "i"},
			},
		})
	}

	aggregation = append(aggregation, bson.M{
		"$limit": 1,
	})

	aggregation = append(aggregation, bson.M{
		"$project": bson.M{
			"_id":       1,
			"member_id": 1,
			"alias":     1,
			"name":      1,
			"address":   1,
			"city":      1,
			"score":     bson.M{"$meta": "searchScore"},
		},
	})

	return aggregation
}

var licenseCache = make(map[string]Roster)

func InLicenseCache(concatDetails string) (Roster, bool) {
	license, ok := licenseCache[concatDetails]
	return license, ok
}

func SearchForMemberLicense(
	db *mongo.Database, gpo, name, addr, city, state string,
) (Roster, error) {
	concatDetails := gpo + name + addr + city + state

	if license, ok := InLicenseCache(concatDetails); ok {
		return license, nil
	}

	should := BuildShouldQuery(name, addr, city, state)

	gpo = RecastGPO(gpo)

	aggregation := BuildAggregationQuery(should, gpo)

	var result []Roster
	cursor, err := db.Collection("roster").Aggregate(context.Background(), aggregation)
	if err != nil {
		licenseCache[concatDetails] = Roster{}
		return Roster{}, err
	}

	if err = cursor.All(context.Background(), &result); err != nil {
		licenseCache[concatDetails] = Roster{}
		return Roster{}, err
	}

	if len(result) == 0 {
		licenseCache[concatDetails] = Roster{}
		return Roster{}, nil
	}

	licenseCache[concatDetails] = result[0]

	return result[0], nil
}
