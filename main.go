package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/spanner"
)

type Community struct {
	DomainName       string
	Description      string
	ShortDescription string
}

func main() {
	projectID := "your-project-id"
	instanceID := "your-instance-id"
	databaseID := "your-database-id"
	csvFilename := "input.csv"

	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID))
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer client.Close()

	file, err := os.Open(csvFilename)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
	}
	defer file.Close()

	r := csv.NewReader(file)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to read from CSV file: %v", err)
		}

		community := Community{
			DomainName:       record[0],
			Description:      record[1],
			ShortDescription: record[2],
		}

		err = updateShortDescription(ctx, client, community)
		if err != nil {
			log.Printf("Failed to update ShortDescription for DomainName %s: %v", community.DomainName, err)
		} else {
			log.Printf("Updated ShortDescription for DomainName %s", community.DomainName)
		}
	}
}

func updateShortDescription(ctx context.Context, client *spanner.Client, community Community) error {
	stmt := spanner.Statement{
		SQL: `UPDATE Communities SET ShortDescription = @shortDescription WHERE DomainName = @domainName`,
		Params: map[string]interface{}{
			"shortDescription": community.ShortDescription,
			"domainName":       community.DomainName,
		},
	}

	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		rowCount, err := txn.Update(ctx, stmt)
		if err != nil {
			return err
		}
		if rowCount == 0 {
			return fmt.Errorf("no rows updated")
		}
		return nil
	})
	return err
}
