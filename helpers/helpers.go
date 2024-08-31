package helpers

import (
	"context"
	"log"
	"math"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Info writes logs in the color blue with "INFO: " as prefix
var Info = log.New(os.Stdout, "\u001b[34mINFO: \u001B[0m", log.LstdFlags|log.Lshortfile)

// Warning writes logs in the color yellow with "WARNING: " as prefix
var Warning = log.New(os.Stdout, "\u001b[33mWARNING: \u001B[0m", log.LstdFlags|log.Lshortfile)

// Error writes logs in the color red with "ERROR: " as prefix
var Error = log.New(os.Stdout, "\u001b[31mERROR: \u001b[0m", log.LstdFlags|log.Lshortfile)

// Debug writes logs in the color cyan with "DEBUG: " as prefix
var Debug = log.New(os.Stdout, "\u001b[36mDEBUG: \u001B[0m", log.LstdFlags|log.Lshortfile)

func GetWorkers(mongoClient *mongo.Client) ([]string, error) {
	// Get all workers from the database
	coll := mongoClient.Database("email_scheduler").Collection("worker_registry")

	// Retrieve the document from the collection
	var result bson.M
	err := coll.FindOne(context.TODO(), bson.D{}).Decode(&result)
	if err != nil {
			log.Fatal(err)
	}

	// Extract and print the workers field
	workers, ok := result["workers"].(bson.A)
	if !ok {
			log.Fatal("workers field is not of type bson.A")
	}

	var workerIds []string

	for _, worker := range workers {
			workerBSON, ok := worker.(bson.M)
			if !ok {
					log.Fatal("worker item is not of type bson.M")
			}

			role, ok := workerBSON["role"].(string)
			if !ok || role != "worker" {
					continue
			}

			id, ok := workerBSON["_id"].(string)
			if !ok {
					log.Fatal("_id field is not of type string")
			}

			workerIds = append(workerIds, id)
	}
	return workerIds, nil
}

func ExponentialBackoffRetry(ctx context.Context, operation func() error, maxRetries int, baseDelay time.Duration, maxDelay time.Duration) error {
	var err error
	for i := 0; i < maxRetries; i++ {
			// Perform the operation
			err = operation()
			if err == nil {
					return nil // Success
			}

			// If context is done, stop retrying
			select {
			case <-ctx.Done():
					return ctx.Err()
			default:
			}

			// Calculate delay for next retry
			delay := time.Duration(math.Pow(2, float64(i))) * baseDelay
			if delay > maxDelay {
					delay = maxDelay
			}

			// Log the retry attempt
			log.Printf("Attempt %d failed: %v. Retrying in %v...", i+1, err, delay)

			// Wait before retrying
			time.Sleep(delay)
	}
	Error.Println("Operation failed after %d attempts: %v", maxRetries, err)
	return nil
}


