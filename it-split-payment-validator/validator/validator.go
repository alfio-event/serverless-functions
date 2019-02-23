package function

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/storage"
)

var db map[string]string
var client *storage.Client
var bucketName string
var ctx = context.Background()

const fileName = "split-payment-def.gz"
const addresseeCodeParam = "addresseeCode"

// init runs during package initialization. So, this will only run during an
// an instance's cold start.
func init() {
	err := performInit()
	if "1" != os.Getenv("TESTING") {
		evaluateErrors(err)
	}

}

func evaluateErrors(err error) {
	if err != nil {
		// waiting to find a way to ignore errors on a test environment...
		log.Fatalf("Error detected %v", err)
	}
}

func performInit() error {
	var err error
	var exists bool
	bucketName, exists = os.LookupEnv("BUCKET_NAME")

	if !exists {
		return errors.New("Cannot retrieve bucket name. Env Variable BUCKET_NAME does not exist")
	}

	client, err = storage.NewClient(ctx)
	if err != nil {
		return err
	}

	return loadDataFromStorage()
}

// SplitPaymentValidator performs a validation on the input param "addresseeCode".
// It returns the validation result and, in case of success, also the "Codice Fiscale" associated with the given addresseeCode
// If the validation is successful, then the VAT will be shown on the invoice, but not included in the
// payment transaction, as per italian's goverment dispositions.
func SplitPaymentValidator(w http.ResponseWriter, r *http.Request) {
	response, err := performValidation(r, db)
	if err != nil {
		w.WriteHeader(400)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(response))
	}
}

func performValidation(r *http.Request, db map[string]string) (string, error) {
	key := r.FormValue(addresseeCodeParam)
	if key != "" {
		cf, exists := db[key]
		if exists {
			return fmt.Sprintf("{\"success\": true, \"fiscalCode\": \"%s\"}", cf), nil
		}
		return fmt.Sprintf("{\"success\": false}"), nil

	}
	return "", errors.New("cannot find input param")
}

func loadDataFromStorage() error {
	bucketReader, err := client.Bucket(bucketName).Object(fileName).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Cannot retrieve data: %v", err)

	}
	reader, err := gzip.NewReader(bucketReader)
	if err != nil {
		return fmt.Errorf("Cannot open GZip stream: %v", err)
	}
	r := csv.NewReader(reader)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("cannot parse csv: %v", err)
		}
		db[record[0]] = record[1]
	}

	return nil
}
