/*
This worker assigns email tasks to workers. It fetches an unassigned task
from the task queue and assigns it using consistent hashing.
*/

package main

import (
	"context"
	"email-scheduler/db"
	"hash/crc32"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	Interval = 1 * time.Minute
)

var (
	logger = logrus.New()
	client *mongo.Client
)

func main() {
	// Setup logging
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)

	// Connect to MongoDB
	var err error
	client, err = db.Connect()
	if err != nil {
		logger.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())

	// Setup ticker and signal handling
	ticker := time.NewTicker(Interval)
	done := make(chan bool)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				assignTask()
			}
		}
	}()

	// Wait for signal
	<-stop
	ticker.Stop()
	close(done)
	wg.Wait()
}

func assignTask() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := getWorkersInfo(ctx)
	if err != nil {
		logger.Errorf("Failed to get workers info: %v", err)
		return
	}

	task, err := fetchAndAssignTask(ctx, info.Workers)
	if err != nil {
		logger.Errorf("Failed to assign task: %v", err)
	}

	if task == nil {
		logger.Info("No tasks available")
		return
	}
}

func getWorkersInfo(ctx context.Context) (*Info, error) {
	var info Info
	coll := client.Database("email_scheduler").Collection("global")
	err := coll.FindOne(ctx, bson.D{{}}).Decode(&info)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

func fetchAndAssignTask(ctx context.Context, workers []string) (*Task, error) {
	coll := client.Database("email_scheduler").Collection("task_queue")
	var task Task

	// Start transaction
	session, err := client.StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)

	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		// Fetch a random task with no assigned worker
		err := coll.FindOneAndUpdate(
			sc,
			bson.D{
				{"status", "READY"},
				{"senddate", bson.D{{"$lte", time.Now()}}},
			},
			bson.D{{"$set", bson.D{{"status", "PROCESSING"}}}},
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		).Decode(&task)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil
			}
			return err
		}

		// Hash task id to get worker
		hash := crc32.ChecksumIEEE([]byte(task.Id.Hex()))
		index := hash % uint32(len(workers))
		assignedWorker := workers[index]

		// Update task with worker
		err = coll.FindOneAndUpdate(
			sc,
			bson.D{{"_id", task.Id}},
			bson.D{{"$set", bson.D{{"worker", assignedWorker}}}},
		).Decode(&task)

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &task, nil
}

type Info struct {
	Master  string
	Workers []string
}

type Task struct {
	Id primitive.ObjectID `bson:"_id"`
}
