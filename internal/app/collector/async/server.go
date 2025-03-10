package asynccollector

import (
	"context"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	log "github.com/sirupsen/logrus"
)

func Run(ctx context.Context, cfg *Config) {
	c, err := newCollector(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to create a new Async Collector instance: %v", err)
		return
	}

	funcframework.RegisterCloudEventFunctionContext(ctx, "/", c.deleteBlobDependencies)

	if err := funcframework.Start(cfg.Port); err != nil {
		log.Fatalf("funcframework.Start: %v\n", err)
	}
}
