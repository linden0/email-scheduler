package task

import (
	"email-scheduler/db"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)


var (
	logger = logrus.New()
	client *mongo.Client
	workerHealthInterval time.Duration = 5 * time.Minute
	masterHealthInterval time.Duration = 5 * time.Minute
	taskProcessingInterval time.Duration = 1 * time.Minute
	role = "worker"
	id = "w5"
)

/*
Worker to assign email tasks to workers
*/
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

	// Load environment variables
	err = godotenv.Load()
	if err != nil {
		logger.Fatalf("Error loading .env file: %v", err)
	}

	// Attempt to claim master role upon startup
	claimMasterRole(client, &role)

	if role == "master" {
		route := gin.Default()
		// Route for workers to respond to health check
		route.GET("/master", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"message": "alive",
			})
		})
		route.Run(":8080")

		// Check health of workers, if down, reassign their tasks
		ticker := time.NewTicker(workerHealthInterval)
		done := make(chan bool)

		go func() {
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					checkWorkers(client, logger)
				}
			}
		}()
	}

	if role == "worker" {
		route := gin.Default()
		// Route for workers to respond to health check
		route.GET("/worker/" + id, func(c *gin.Context) {
			c.JSON(200, gin.H{
				"message": "alive",
			})
		})

		// Channels to control tickers
		stopTaskProcessing := make(chan bool)
		stopHeartbeatCheck := make(chan bool)

		// Move tasks to processing queue for execution workers
		go startTaskProcessingTicker(
			stopTaskProcessing, 
			taskProcessingInterval, 
			id, 
			client,
			logger,
		)
		// Check health of master
		go startHeartbeatTicker(
			stopTaskProcessing, 
			stopHeartbeatCheck, 
			masterHealthInterval, 
			client, 
			&role,
		)

		route.Run(":8080")
	}
}		




