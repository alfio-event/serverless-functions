package function

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

const fileName = "split-payment-def.gz"

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// OpenDataLoader is a function triggered by a GCP Pub/Sub event, that loads and parses content
// from the Italian Government's Open Data directory (https://www.indicepa.gov.it/documentale/n-opendata.php)
func OpenDataLoader(ctx context.Context, m PubSubMessage) error {

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
		return err
	}

	bucketName, exists := os.LookupEnv("BUCKET_NAME")

	if !exists {
		return errors.New("Cannot retrieve bucket name. Env Variable BUCKET_NAME does not exist")
	}

	resp, err := http.Get("https://www.indicepa.gov.it/public-services/opendata-read-service.php?dstype=FS&filename=serv_fatt.txt")
	if err != nil {
		log.Fatalf("Cannot get file: %v", err)
		return err
	}
	defer resp.Body.Close()

	content := processFileContent(resp.Body)

	t := time.Now().UTC()
	wc := client.Bucket(bucketName).Object(fileName).NewWriter(ctx)
	wc.ContentType = "application/gzip"
	wc.Metadata = map[string]string{
		"creation-date": t.String(),
	}

	writer := gzip.NewWriter(wc)
	err = createCSV(writer, content)

	if err != nil {
		log.Fatalf("failed to create CSV: %v", err)
		return err
	}

	err = writer.Close()
	if err != nil {
		log.Fatalf("failed to flush GZip stream: %v", err)
		return err
	}

	err = wc.Close()
	if err != nil {
		log.Fatalf("failed to close GCS stream: %v", err)
		return err
	}

	log.Printf("File created successfully at %v", t.String())

	return err
}

func processFileContent(src io.Reader) [][]string {
	var (
		row     int
		content [][]string
	)

	rowScanner := bufio.NewScanner(src)

	// The file contains the following information:
	// 12 colums:
	// 		cod_amm, cod_ou, cod_uni_ou, des_ou, regione, provincia, comune, indirizzo, cap, cf, dt_verifica_cf, data_avvio_sfe
	// for our purpose of validating if a given 6-chars E-Billing Addressee Code is valid, we need only:
	// 	- cod_uni_ou, which is the Addressee Code
	// 	- cf, which is the "Codice Fiscale" (a sort of Tax ID, but not really)

	for rowScanner.Scan() {
		if row > 0 {
			row := strings.Split(rowScanner.Text(), "\t")
			parsedRow := []string{row[2], row[9]}
			content = append(content, parsedRow)
		}
		row++
	}
	return content
}

func createCSV(wc io.Writer, content [][]string) error {
	csvW := csv.NewWriter(wc)
	for _, record := range content {
		if err := csvW.Write(record); err != nil {
			return err
		}
	}
	csvW.Flush()
	return nil
}
