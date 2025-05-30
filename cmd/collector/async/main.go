package main

import (
	"context"
	"flag"
	"io"
	"os"

	asynccollector "github.com/googleforgames/open-saves/internal/app/collector/async"
	"github.com/googleforgames/open-saves/internal/pkg/cmd"
	log "github.com/sirupsen/logrus"
)

func main() {
	defaultPort := cmd.GetEnvVarString("OPEN_SAVES_ASYNC_COLLECTOR_PORT", "8080")
	defaultCloud := cmd.GetEnvVarString("OPEN_SAVES_CLOUD", "gcp")
	defaultBucket := cmd.GetEnvVarString("OPEN_SAVES_BUCKET", "gs://triton-dev-store")
	defaultProject := cmd.GetEnvVarString("OPEN_SAVES_PROJECT", "triton-for-games-dev")
	defaultLogLevel := cmd.GetEnvVarString("OPEN_SAVES_LOG_LEVEL", "info")
	defaultLogFormat := cmd.GetEnvVarString("OPEN_SAVES_LOG_FORMAT", "json")
	defaultLogFile := cmd.GetEnvVarString("OPEN_SAVES_LOG_FILE", "")
	defaultDatastoreTXMaxRetries := cmd.GetEnvVarUInt("OPEN_SAVES_DATASTORE_TX_MAX_RETRIES", 1)

	var (
		port                  = flag.String("port", defaultPort, "Port where new events will be listened for")
		cloud                 = flag.String("cloud", defaultCloud, "The public cloud provider you wish to run Open Saves on")
		bucket                = flag.String("bucket", defaultBucket, "The bucket which will hold Open Saves blobs")
		project               = flag.String("project", defaultProject, "The GCP project ID to use for Datastore")
		logLevel              = flag.String("log-level", defaultLogLevel, "Minimum Log level")
		logFormat             = flag.String("log-format", defaultLogFormat, "Minimum Log format")
		logFile               = flag.String("log-file", defaultLogFile, "Log file to write the logs, if missing then it will write into stderr")
		datastoreTXMaxRetries = flag.Uint("datastore-tx-max-retries", uint(defaultDatastoreTXMaxRetries), "Max retries attempt when using Datastore TX")
	)

	flag.Parse()

	cfg := &asynccollector.Config{
		Port:                  *port,
		Cloud:                 *cloud,
		Bucket:                *bucket,
		Project:               *project,
		LogLevel:              *logLevel,
		DatastoreTXMaxRetries: *datastoreTXMaxRetries,
	}

	// Initialize writing the logs into a file.
	// This must happen before any other log entry is created to maximize writing all logs into the file.
	if *logFile != "" {
		// Open the log file
		file, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Warnf("Failed to open log file %s", *logFile)
		} else {
			defer file.Close()
			// Allow to write to Stderr at the same time as the logfile.
			multiWriter := io.MultiWriter(os.Stderr, file)
			log.SetOutput(multiWriter)
		}
	}

	// Configure the log format.
	switch *logFormat {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
	case "text":
		log.SetFormatter(&log.TextFormatter{})
	default:
		log.Warnf("Unknown log format %s", *logFormat)
	}

	ctx := context.Background()
	asynccollector.Run(ctx, cfg)
}
