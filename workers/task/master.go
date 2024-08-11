package task

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


func checkWorkers(client *mongo.Client, logger *logrus.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get list of workers
	type Info struct {
		Workers []string `bson:"workers"`
	}
	var info Info
	coll := client.Database("email_scheduler").Collection("global")
	err := coll.FindOne(ctx, bson.D{{}}).Decode(&info)
	if err != nil {
		logger.Errorf("Failed to get workers info: %v", err)
		return
	}

	// Check health of workers
	for _, worker := range info.Workers {
		// Ping worker
		workerURL := os.Getenv("API_HOST") + worker
		response, err := http.Get(workerURL)
		if err != nil || response.StatusCode != http.StatusOK {
			logger.Warnf("Worker %s is down or unresponsive", worker)
			reassignTasks(ctx, client, worker)
		} else {
			logger.Infof("Worker %s is healthy", worker)
		}
		if response != nil {
			response.Body.Close()
		}
	}
}

func reassignTasks(ctx context.Context, client *mongo.Client, worker string) {
	type Task struct {
		Id string `bson:"_id"`
	}
	coll := client.Database("email_scheduler").Collection("task_queue")
	cursor, err := coll.Find(ctx, bson.D{{"worker", worker}})
	if err != nil {
		logger.Errorf("Failed to find tasks for worker %s: %v", worker, err)
		return
	}
	defer cursor.Close(ctx)

	// Reassign tasks to READY status, for a new worker to pick up
	for cursor.Next(ctx) {
		var task Task
		if err := cursor.Decode(&task); err != nil {
			logger.Errorf("Failed to decode task: %v", err)
			continue
		}

		_, err := coll.UpdateOne(
			ctx,
			bson.D{{"_id", task.Id}},
			bson.D{{"$set", bson.D{{"status", "READY"}}}},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			logger.Errorf("Failed to update task %s status to READY: %v", task.Id, err)
		} else {
			logger.Infof("Task %s reassigned to READY status", task.Id)
		}
	}

	if err := cursor.Err(); err != nil {
		logger.Errorf("Cursor error: %v", err)
	}
}
