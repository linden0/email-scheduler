package db

import (
	"context"
	"log"
	"os"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


func Connect() (*mongo.Client, error) {
   

    err := godotenv.Load()
    if err != nil {
        log.Fatalf("Error loading .env file: %v", err)
    }
    opts := options.Client().ApplyURI(os.Getenv("MONGODB_URI"))

    // Create a new client and connect to the server
    client, err := mongo.Connect(context.TODO(), opts)
    if err != nil {
        log.Fatalf("Failed to connect to MongoDB: %v", err)
    }

    // Send a ping to confirm a successful connection
    if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Err(); err != nil {
        panic(err)
    }
    log.Println("Connected to MongoDB!")
    return client, nil
}