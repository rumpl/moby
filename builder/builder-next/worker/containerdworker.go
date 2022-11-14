package worker

import (
	"context"

	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/exporter"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/worker/base"
)

// ContainerdWorker is a local worker instance with dedicated snapshotter, cache, and so on.
type ContainerdWorker struct {
	*base.Worker
}

// NewContainerdWorker instantiates a local worker.
func NewContainerdWorker(ctx context.Context, wo base.WorkerOpt) (*ContainerdWorker, error) {
	bw, err := base.NewWorker(ctx, wo)
	if err != nil {
		return nil, err
	}
	return &ContainerdWorker{Worker: bw}, nil
}

// Exporter returns exporter by name
func (w *ContainerdWorker) Exporter(name string, sm *session.Manager) (exporter.Exporter, error) {
	switch name {
	// TODO(thaJeztah): this should be a const: https://github.com/moby/moby/pull/44079#discussion_r1010894271
	// Should this use [client.ExporterDocker] ? It also looks like we're
	// using two different approaches; a "wrapped" Worker (ContainerdWorker)
	// and updating the exporter-name before calling; we should pick one.
	case "moby":
		return w.Worker.Exporter(client.ExporterImage, sm)
	default:
		return w.Worker.Exporter(name, sm)
	}
}
