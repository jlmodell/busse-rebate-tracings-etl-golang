package helpers

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func InsertManyRecords[T any](ctx context.Context, collection *mongo.Collection, documents []T) error {
	var interfaceSlice []interface{} = make([]interface{}, len(documents))
	for i, d := range documents {
		interfaceSlice[i] = d
	}

	result, err := collection.InsertMany(ctx, interfaceSlice)
	if err != nil {
		log.Printf("Failed to insert documents: %v", err)
		return err
	}

	log.Printf("Inserted %v documents", len(result.InsertedIDs))

	return nil
}

func DeleteManyByKey(ctx context.Context, collection *mongo.Collection, key string, value interface{}) error {
	filter := bson.M{key: value}
	result, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		log.Printf("Failed to delete documents: %v", err)
		return err
	}

	log.Printf("Deleted %v documents", result.DeletedCount)

	return nil
}
