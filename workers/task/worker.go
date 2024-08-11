package task

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func startTaskProcessingTicker(stopTaskProcessing chan bool, taskProcessingInterval time.Duration, workerId string, client *mongo.Client, logger *logrus.Logger) {
	ticker := time.NewTicker(taskProcessingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopTaskProcessing:
			return
		case <-ticker.C:
			processTasks(client, workerId, logger)
		}
	}

}

func processTasks(client *mongo.Client, workerId string, logger *logrus.Logger) {
	ctx := context.TODO()

	session, err := client.StartSession()
	if err != nil {
			logger.Fatalf("Failed to start session: %v", err)
	}
	defer session.EndSession(ctx)

	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		// Find tasks assigned to worker
		coll := client.Database("email_scheduler").Collection("task_queue")
		cursor, err := coll.Find(
				sessCtx,
				bson.D{
						{"senddate", bson.D{{"$lte", time.Now()}}},
						{"worker", workerId},
				},
		)
		if err != nil {
				return nil, err
		}
		defer cursor.Close(sessCtx)
		
		var tasks []interface{}
		if err = cursor.All(sessCtx, &tasks); err != nil {
				return nil, err
		}

		if len(tasks) == 0 {
				return nil, nil
		}

		// Copy tasks to send_queue
		sendQueueColl := client.Database("email_scheduler").Collection("send_queue")
		_, err = sendQueueColl.InsertMany(sessCtx, tasks)
		if err != nil {
				return nil, err
		}

		// Remove tasks from task_queue
		taskIDs := make([]primitive.ObjectID, len(tasks))
		for i, task := range tasks {
				taskIDs[i] = task.(bson.M)["_id"].(primitive.ObjectID)
		}
		// Delete tasks from task_queue
		_, err = coll.DeleteMany(sessCtx, bson.M{"_id": bson.M{"$in": taskIDs}})
		if err != nil {
				return nil, err
		}

		return nil, nil
	}

	_, err = session.WithTransaction(ctx, callback)
	if err != nil {
			logger.Fatalf("Failed to process tasks: %v", err)
			return
	}

}


func startHeartbeatTicker(stopTaskProcessing chan bool, stopHeartbeatCheck chan bool, masterHealthInterval time.Duration, client *mongo.Client, role *string) {
	ticker := time.NewTicker(masterHealthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopHeartbeatCheck:
			fmt.Println("Heartbeat check stopped.")
			return
		case <-ticker.C:
			checkHeartbeat(client, role, logger)
		}
	}
}


func checkHeartbeat(client *mongo.Client, role *string, logger *logrus.Logger) {
	response, err := http.Get(os.Getenv("API_HOST") + "/master")
	if err != nil || response.StatusCode != 200 {
		// Try to claim and re-elect master
		isClaimSuccess := claimMasterRole(client, role)
		if isClaimSuccess {
			// Assign current workers task
			assignTasks(client, id)
			// Restart task.go (container is configured to restart on non problematic exit)
			// This allows the elected worker to be correctly re-initilaized as the master role
			os.Exit(0)

		}
	}
} 

func claimMasterRole(client *mongo.Client, role *string) bool {
	type Info struct {
		Workers []string `bson:"workers"`
	}
	var info Info
	coll := client.Database("email_scheduler").Collection("global")
	err := coll.FindOneAndUpdate(
		context.TODO(),
		bson.D{{"claimable", true}},
		bson.D{{"$set", bson.D{{"claimable", false}}}},
	).Decode(&info)
	
	if err != nil {
		return false
	}

	// Successfully claimed master role
	*role = "master"
	// Remove worker from list, as it is now the master
	isWorkerUsed := checkWorkerUsed(info.Workers, id)
	if isWorkerUsed == true {
		err = coll.FindOneAndUpdate(
			context.TODO(),
			bson.D{{}},
			bson.D{{"$set", bson.D{{"workers", removeWorker(info.Workers, id)}}}},
		).Decode(&info)
	}

	return true
}

func assignTasks(client *mongo.Client, workerId string) {
	// Get workers
	type Info struct {
		Master string
		Workers []string
	}
	var info Info
	coll := client.Database("email_scheduler").Collection("global")
	err := coll.FindOne(context.TODO(), bson.D{{}}).Decode(&info)
	if err != nil {
		panic(err)
	}

	collection := client.Database("email_scheduler").Collection("task_queue")
	filter := bson.M{ "worker": workerId}
    cursor, err := collection.Find(context.TODO(), filter)
    if err != nil {
			log.Fatal(err)
    }
    var documents []bson.M
    if err = cursor.All(context.TODO(), &documents); err != nil {
			log.Fatal(err)
    }

    // Assign documents a new worker
    for _, doc := range documents {
			id := doc["_id"].(string)
			doc["worker"] = getWorker(info.Workers, workerId, id)
    }

    // Step 3: Create bulk update operations
    var models []mongo.WriteModel
    for _, doc := range documents {
        models = append(models, mongo.NewUpdateOneModel().
            SetFilter(bson.M{"_id": doc["_id"]}).
            SetUpdate(bson.M{"$set": doc}))
    }

    // Step 4: Execute bulk operations
    if len(models) > 0 {
        bulkOption := options.BulkWrite().SetOrdered(false)
        bulkResult, err := collection.BulkWrite(context.TODO(), models, bulkOption)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Printf("%d documents matched, %d documents modified\n", bulkResult.MatchedCount, bulkResult.ModifiedCount)
    } else {
        fmt.Println("No documents to update")
    }

	
}

func getWorker(workers []string, workerId string, taskId string) string {
	hash :=crc32.ChecksumIEEE([]byte(taskId))
	index := hash % uint32(len(workers))
	// Ensure task is not assigned to the same worker
	if workers[index] == workerId {
		index = (index + 1) % uint32(len(workers))
	}
	return workers[index]
}