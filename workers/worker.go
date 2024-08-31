package main

import (
	"context"
	"email-scheduler/db"
	"email-scheduler/helpers"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	REGISTRY_DOCUMENT_ID = "66d230c448aa0835e07359e0"
	HEALTH_CHECK_INTERVAL = 30 * time.Second
	PROCESS_TASK_INTERVAL = 10 * time.Second
)

var server *http.Server

type Task struct {
	Id primitive.ObjectID `bson:"_id"`
	Recipient string `bson:"recipient"`
	Content string `bson:"content"`
	Due time.Time `bson:"due"`
	Worker string `bson:"worker"`
	Status string `bson:"status"`
}

func main() {
	// Connect to MongoDB
	var err error
	client, err := db.Connect()
	if err != nil {
		log.Fatal(err)
	}

	// Attempt to claim master
	role := "worker"
	isClaimMaster := claimMaster(client)
	if isClaimMaster {
		role = "master"
	}

	// Generate worker ID
	workerID := primitive.NewObjectID().Hex()
	// Add worker to worker_registry
	newWorker := bson.D{
		{"_id", workerID},
		{"role", role},
	}
	registryId, _ := primitive.ObjectIDFromHex(REGISTRY_DOCUMENT_ID)
	coll := client.Database("email_scheduler").Collection("worker_registry")
	_, err = coll.UpdateOne(
		context.TODO(),
		bson.D{{"_id", registryId}},
		bson.D{{"$push", bson.D{{"workers", newWorker}}}},
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Initialized new %s with ID: %s", role, workerID)

	// Initialize worker context
	ctx, cancel := context.WithCancel(context.Background())

	// Perform worker duties
	if role == "worker" {
		startWorkerServer(workerID)
		go processTask(client, workerID, ctx)
		go checkMaster(client, ctx, cancel)
	}

	if role == "master" {
		startMasterServer()
		go checkWorkers(client)
	}

	select {}
}

func startMasterServer() {
	log.Println("Starting master server")
	router := gin.Default()
	router.GET("/master", healthHandler)
	server = &http.Server{Addr: ":8080", Handler: router}
	go server.ListenAndServe()
}

func startWorkerServer(workerId string) {
	log.Println("Starting worker server")
	router := gin.Default()
	router.GET("/"+ workerId + "/health", healthHandler)
	server = &http.Server{Addr: ":8080", Handler: router}
	go server.ListenAndServe()
}

func healthHandler(c *gin.Context) {
	c.JSON(200, gin.H{
		"status": "healthy",
	})
}

func processTask(client *mongo.Client, workerId string, ctx context.Context) {
	for {
		select {
			case <- ctx.Done():
				return
			case <-time.After(PROCESS_TASK_INTERVAL):
				log.Println("Processing task")
				// Fetch a task from task_queue
				coll := client.Database("email_scheduler").Collection("task_queue")
				filter := bson.D{{"status", "QUEUED"}, {"worker", workerId}, {"due", bson.D{{"$lte", time.Now()}}}}
				var task Task
				err := coll.FindOne(context.TODO(), filter).Decode(&task)
				if err == mongo.ErrNoDocuments {
					continue
				}
				if err != nil {
					log.Fatal(err)
				}
				// Process the task with exponential backoff retry
				processCtx := context.Background()
				helpers.ExponentialBackoffRetry(processCtx, func () error { return nil }, 4, 2 * time.Second, 10 * time.Second)
				log.Println("Processed task:", task.Id.Hex())
				// Update task status
				_, err = coll.UpdateOne(
					context.TODO(),
					bson.D{{"_id", task.Id}},
					bson.D{{"$set", bson.D{{"status", "SENT"}}}},
				)
				if err != nil {
					log.Fatal(err)
				}
		}
	}
}

func checkMaster(client *mongo.Client, ctx context.Context, cancel context.CancelFunc) {
	for {
		select {
			case <- ctx.Done():
				return
			case <-time.After(HEALTH_CHECK_INTERVAL):
				log.Println("Checking master")
				// Check if master is alive
				_, err := http.Get("http://localhost:8080/master")
				if err != nil {
					isClaimMaster := claimMaster(client)
					if isClaimMaster {
						// Remove master from worker_registry and set isMasterClaimable to true
						coll := client.Database("email_scheduler").Collection("worker_registry")
						registryId, _ := primitive.ObjectIDFromHex(REGISTRY_DOCUMENT_ID)
						_, err = coll.UpdateOne(
							context.TODO(),
							bson.D{{"_id", registryId}},
							bson.D{
									{"$pull", bson.D{{"workers", bson.D{{"role", "master"}}}}},
									{"$set", bson.D{{"isMasterClaimable", true}}},
							},
						)
						if err != nil {
							log.Fatal(err)
						}
						// Cancel worker context to stop all worker processes
						cancel()
						server.Shutdown(context.Background())
						// Start master processes
						startMasterServer()
						go checkWorkers(client)
						return
					}
				}
		}
	}
}


func checkWorkers(client *mongo.Client) {
	for {
		select {
			case <-time.After(HEALTH_CHECK_INTERVAL):
				// Get workers
				workers, err := helpers.GetWorkers(client)
				if err != nil {
					log.Fatal(err)
				}
				for _, worker := range workers {
					_, err := http.Get("http://localhost:8080/" + worker + "/health")
					if err != nil {
						// Remove worker from worker_registry
						coll := client.Database("email_scheduler").Collection("worker_registry")
						registryId, _ := primitive.ObjectIDFromHex(REGISTRY_DOCUMENT_ID)
						_, err = coll.UpdateOne(
							context.TODO(),
							bson.D{{"_id", registryId}},
							bson.D{
									{"$pull", bson.D{{"workers", bson.D{{"_id", worker}}}}},
							},
						)
						// Get updated workers
						workers, err := helpers.GetWorkers(client)
						if err != nil {
							log.Fatal(err)
						}
						// Re assign tasks to other workers
						roundRobinCount := 0
						workerCount := len(workers)
						operations := []mongo.WriteModel{}
						coll = client.Database("email_scheduler").Collection("task_queue")
						cursor, err := coll.Find(context.TODO(), bson.D{{"status", "QUEUED"}, {"worker", worker}})
						if err != nil {
							log.Fatal(err)
						}
						defer cursor.Close(context.Background())
						for cursor.Next(context.Background()) {
							var task Task
							cursor.Decode(&task)
							roundRobinCount = roundRobinCount % workerCount
							operations = append(operations, mongo.NewUpdateOneModel().
								SetFilter(bson.D{{"_id", task.Id}}).
								SetUpdate(bson.D{{"$set", bson.D{{"worker", workers[roundRobinCount]}}}}))
							roundRobinCount++
						}
						if _, err := coll.BulkWrite(context.TODO(), operations); err != nil {
							log.Fatal(err)
						}
					}
				}
		}
	}
}

func claimMaster(client *mongo.Client) bool {
	coll := client.Database("email_scheduler").Collection("worker_registry")
	registryId, _ := primitive.ObjectIDFromHex(REGISTRY_DOCUMENT_ID)
	result, err := coll.UpdateOne(
		context.TODO(),
		bson.D{{"_id", registryId}, {"isMasterClaimable", true}},
		bson.D{{"$set", bson.D{{"isMasterClaimable", false}}}},
	)
	if err != nil {
		log.Fatal(err)
	}
	if result.ModifiedCount == 1 {
		log.Println("Claimed master")
		return true
	}
	return false
}