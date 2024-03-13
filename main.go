package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	instance "github.com/jlmodell/busse-rebate-tracings-etl-golang/generic_interfaces"
	helpers "github.com/jlmodell/busse-rebate-tracings-etl-golang/helpers"
	models "github.com/jlmodell/busse-rebate-tracings-etl-golang/models"
	"github.com/joho/godotenv"
)

var (
	Db       *mongo.Database
	fp       string
	Customer string
	fileDate time.Time
	fileName string
)

const customers = "atlantic, dealmed, trianim, tri-anim, henryschein, mckesson, mgm, twinmed, concordance, concordance_mms, mms"

// Connect connects to the MongoDB database and returns a MongoDB instance
func init() {
	var err error

	err = godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	customerPtr := flag.String("customer", "", "The customer ID (short: -c)")
	customerShortPtr := flag.String("c", "", "The customer ID (alternative to --customer)")

	filepathPtr := flag.String("filepath", "", "Path to the file (short: -f)")
	datePtr := flag.String("date", "", "The date in YYYY-MM-DD format (short: -d)")

	// Short flag alternatives (manual workaround)
	filepathShortPtr := flag.String("f", "", "Path to the file (alternative to --filepath)")
	dateShortPtr := flag.String("d", "", "The date in YYYY-MM-DD format (alternative to --date)")

	flag.Parse()

	if *customerPtr == "" && *customerShortPtr != "" {
		customerPtr = customerShortPtr
	}

	if *filepathPtr == "" && *filepathShortPtr != "" {
		filepathPtr = filepathShortPtr
	}
	if *datePtr == "" && *dateShortPtr != "" {
		datePtr = dateShortPtr
	}

	if *datePtr != "" {
		log.Println("Date provided:", *datePtr)
		_, err := time.Parse("2006-01-02", *datePtr)
		if err != nil {
			log.Fatal("Error parsing date:", err)
		}
	}

	Customer = strings.ToLower(*customerPtr)

	// validate Customer is in the list of customers
	if !strings.Contains(customers, Customer) {
		log.Fatalf("Invalid customer. Must be one of: %s", customers)
	}

	fp = *filepathPtr
	fileDate, _ = time.Parse("2006-01-02", *datePtr)

	log.Println("File path provided:", fp)
	log.Println("Date provided:", fileDate)

	if fp == "" {
		log.Fatal("No file path provided.")
	}

	fileName = strings.Split(fp, "/")[len(strings.Split(fp, "/"))-1]

	if fileDate.IsZero() {
		log.Println("No date provided.")
		return
	}

	uri := os.Getenv("MONGODB_URI")
	dbName := "busserebatetraces"

	ctx, cancel := context.WithTimeout(context.Background(), 3600*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Printf("failed to connect to MongoDB: %v", err)
	}

	// Check the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Printf("failed to ping MongoDB: %v", err)
	}

	Db = client.Database(dbName)

}

func main() {
	var err error

	csvFile := ""

	// convert to csv
	if strings.Contains(fp, ".xls") {
		csvFile, err = helpers.ConvertToCSV(fp)
		if err != nil {
			log.Fatal("Error converting to CSV. -> ", err)
		}
	} else {
		csvFile = fp
	}

	// trying to clean up the switch statement

	// var recordInstance interface{}

	// switch Customer {
	// case "dealmed":
	// 	recordInstance := &models.Dealmed{}
	// case "henryschein":
	// 	recordInstance := &models.Henryschein{}
	// case "trianim":
	// 	recordInstance := &models.TriAnim{}
	// case "tri-anim": // tri-anim is the same as trianim
	// 	recordInstance := &models.TriAnim{}
	// case "mckesson":
	// 	recordInstance := &models.Mckesson{}
	// case "mgm":
	// 	recordInstance := &models.Mckesson{}
	// case "twinmed":
	// 	recordInstance := &models.Twinmed{}
	// case "concordance":
	// 	recordInstance := &models.Concordance{}
	// case "concordance_mms":
	// 	recordInstance := &models.ConcordanceMMS{}
	// case "mms": // mms is the same as concordance_mms
	// 	recordInstance := &models.ConcordanceMMS{}
	// case "atlantic":
	// 	recordInstance := &models.Atlantic{}
	// default:
	// 	log.Fatal("Invalid customer. Must be one of: ", customers)
	// }

	// records, err := instance.ReadCSVStruct(csvFile, recordInstance)
	// if err != nil {
	// 	log.Fatal("Error reading CSV. -> ", err)
	// }

	// tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

	// collection := Db.Collection("tracings")

	// key := tracings[0].GetKey()

	// err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
	// if err != nil {
	// 	log.Fatal("Error deleting records. -> ", err)
	// }

	// err = helpers.InsertManyRecords(context.Background(), collection, tracings)
	// if err != nil {
	// 	log.Fatal("Error inserting records. -> ", err)
	// }

	// TODO: clean up switch statement
	switch Customer {
	case "dealmed":
		recordInstance := &models.Dealmed{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	case "henryschein":
		recordInstance := &models.Henryschein{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	case "tri-anim":
		recordInstance := &models.TriAnim{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	case "trianim":
		recordInstance := &models.TriAnim{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}

	case "mckesson":
		recordInstance := &models.Mckesson{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	case "mgm":
		recordInstance := &models.Mckesson{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	case "twinmed":
		recordInstance := &models.Twinmed{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	case "concordance":
		recordInstance := &models.Concordance{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	case "concordance_mms":
		recordInstance := &models.ConcordanceMMS{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	case "mms":
		recordInstance := &models.ConcordanceMMS{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	case "atlantic":
		recordInstance := &models.Atlantic{}
		records, err := instance.ReadCSVStruct(csvFile, recordInstance)
		if err != nil {
			log.Fatal("Error reading CSV. -> ", err)
		}

		tracings := instance.EnhanceRecords(records, fileDate, fileName, Db)

		collection := Db.Collection("tracings")

		key := tracings[0].GetKey()

		err = helpers.DeleteManyByKey(context.Background(), collection, "period", key)
		if err != nil {
			log.Fatal("Error deleting records. -> ", err)
		}

		err = helpers.InsertManyRecords(context.Background(), collection, tracings)
		if err != nil {
			log.Fatal("Error inserting records. -> ", err)
		}
	}

	// upload raw records to mongodb data warehouse
	instance.UploadToMongoDBUsingPython(fileDate.Format("01"), fileDate.Format("2006"), fp)
}
