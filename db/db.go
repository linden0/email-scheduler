package db

import (
	"context"
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
    logger = logrus.New()
)

func Connect() (*mongo.Client, error) {
    // Setup logging
    logger.SetFormatter(&logrus.JSONFormatter{})
    logger.SetOutput(os.Stdout)
    logger.SetLevel(logrus.InfoLevel)

    err := godotenv.Load()
    if err != nil {
        logger.Fatalf("Error loading .env file: %v", err)
    }
    opts := options.Client().ApplyURI(os.Getenv("MONGO_URI"))

    // Create a new client and connect to the server
    client, err := mongo.Connect(context.TODO(), opts)
    if err != nil {
        logger.Fatalf("Failed to connect to MongoDB: %v", err)
    }

    // Send a ping to confirm a successful connection
    if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Err(); err != nil {
        panic(err)
    }
    logger.Info("Successfully connected to MongoDB")

    return client, nil
}