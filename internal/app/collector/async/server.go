package asynccollector

import (
	"context"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	"github.com/cloudevents/sdk-go/v2/event"
	log "github.com/sirupsen/logrus"
)

func Run(ctx context.Context, cfg *Config) {

	c, err := newCollector(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to create a new Async Collector instance: %v", err)
		return
	}

	funcframework.RegisterCloudEventFunctionContext(ctx, "/", logErrorIfApply(c.deleteBlobDependencies))

	if err := funcframework.Start(cfg.Port); err != nil {
		log.Fatalf("funcframework.Start: %v\n", err)
	}
}

func logErrorIfApply(chainFunc func(context.Context, event.Event) error) func(context.Context, event.Event) error {
	return func(ctx context.Context, e event.Event) error {
		err := chainFunc(ctx, e)
		if err != nil {
			log.Errorf("event %s failed with error: %v", string(e.Data()), err)
		}

		return err
	}
}
