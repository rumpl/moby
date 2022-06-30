package containerdstore

import (
	"context"
	"fmt"

	"github.com/containerd/containerd/content"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// TODO: handle pruneFilters
func (cs *containerdStore) ImagesPrune(ctx context.Context, pruneFilters filters.Args) (*types.ImagesPruneReport, error) {
	is := cs.client.ImageService()
	store := cs.client.ContentStore()

	images, err := is.List(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to list images")
	}

	platform := platforms.DefaultStrict()
	report := types.ImagesPruneReport{}
	toDelete := map[digest.Digest]uint64{}
	errs := []error{}

	for _, img := range images {
		err := getContentDigestsWithSizes(ctx, img, store, platform, toDelete)
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}

	for digest, size := range toDelete {
		err := store.Delete(ctx, digest)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "Failed to delete %s", digest.String()))
		}
		report.SpaceReclaimed += size
		report.ImagesDeleted = append(report.ImagesDeleted,
			types.ImageDeleteResponseItem{
				Deleted: digest.String(),
			},
		)
	}

	for _, img := range images {
		err = is.Delete(ctx, img.Name, containerdimages.SynchronousDelete())
		if err != nil {
			errs = append(errs, err)
			continue
		}

		report.ImagesDeleted = append(report.ImagesDeleted,
			types.ImageDeleteResponseItem{
				Untagged: img.Name,
			},
		)
	}

	if len(errs) > 1 {
		return &report, MultipleErrors{all: errs}
	} else if len(errs) == 1 {
		return &report, errs[0]
	}

	return &report, nil
}

func getContentDigestsWithSizes(ctx context.Context, img containerdimages.Image, store content.Store, platform platforms.MatchComparer, toDelete map[digest.Digest]uint64) error {
	return containerdimages.Walk(ctx, containerdimages.Handlers(containerdimages.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		if desc.Size < 0 {
			return nil, fmt.Errorf("invalid size %v in %v (%v)", desc.Size, desc.Digest, desc.MediaType)
		}
		toDelete[desc.Digest] = uint64(desc.Size)
		return nil, nil
	}), containerdimages.LimitManifests(containerdimages.FilterPlatforms(containerdimages.ChildrenHandler(store), platform), platform, 1)), img.Target)
}

type MultipleErrors struct {
	all []error
}

func (m MultipleErrors) Error() string {
	errString := ""
	for _, err := range m.all {
		if len(m.all) > 0 {
			errString += "\n"
		}
		errString += err.Error()
	}

	return errString
}
