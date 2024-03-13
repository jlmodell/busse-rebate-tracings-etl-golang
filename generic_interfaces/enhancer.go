package instance

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	models "github.com/jlmodell/busse-rebate-tracings-etl-golang/models"
)

type RecordEnhancer interface {
	GetRebateAmount() float64
	ToTracingWithEnrichment(fileDate time.Time, db *mongo.Database) models.Tracing
}

func EnhanceRecords[T RecordEnhancer](records []T, fileDate time.Time, db *mongo.Database) []models.Tracing {
	var sum float64 = 0.0
	var tracings []models.Tracing

	for _, record := range records {
		sum += record.GetRebateAmount()
		tracing := record.ToTracingWithEnrichment(fileDate, db)
		tracings = append(tracings, tracing)
	}

	fmt.Println("\nSum:", sum)

	return tracings
}
