package containerd

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/distribution/reference"
	containertypes "github.com/docker/docker/api/types/container"
	imagetype "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/daemon/images"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/image"
	"github.com/docker/docker/layer"
	"github.com/docker/go-connections/nat"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

var truncatedID = regexp.MustCompile(`^([a-f0-9]{4,64})$`)

// GetImage returns an image corresponding to the image referred to by refOrID.
func (i *ImageService) GetImage(ctx context.Context, refOrID string, options imagetype.GetImageOpts) (*image.Image, error) {
	containerdImage, img, err := i.getImage(ctx, refOrID, options.Platform)
	if err != nil {
		return nil, err
	}

	if options.Details {
		size, err := containerdImage.Size(ctx)
		if err != nil {
			return nil, err
		}

		tagged, err := i.client.ImageService().List(ctx, "target.digest=="+containerdImage.Target().Digest.String())
		if err != nil {
			return nil, err
		}
		tags := make([]reference.Named, 0, len(tagged))
		for _, i := range tagged {
			name, err := reference.ParseNamed(i.Name)
			if err != nil {
				return nil, err
			}
			tags = append(tags, name)
		}

		img.Details = &image.Details{
			References:  tags,
			Size:        size,
			Metadata:    nil,
			Driver:      i.snapshotter,
			LastUpdated: containerdImage.Metadata().UpdatedAt,
		}
	}
	return img, nil
}

func (i *ImageService) getImage(ctx context.Context, refOrID string, platform *ocispec.Platform) (containerd.Image, *image.Image, error) {
	ctrdimg, err := i.resolveImage(ctx, refOrID, platform)
	if err != nil {
		return nil, nil, err
	}

	containerdImage := containerd.NewImage(i.client, ctrdimg)
	if platform != nil {
		containerdImage = containerd.NewImageWithPlatform(i.client, ctrdimg, platforms.OnlyStrict(*platform))
	}
	provider := i.client.ContentStore()
	conf, err := ctrdimg.Config(ctx, provider, containerdImage.Platform())
	if err != nil {
		return nil, nil, err
	}

	imageConfigBytes, err := content.ReadBlob(ctx, containerdImage.ContentStore(), conf)
	if err != nil {
		return nil, nil, err
	}

	var ociimage ocispec.Image
	if err := json.Unmarshal(imageConfigBytes, &ociimage); err != nil {
		return nil, nil, err
	}

	fs, err := containerdImage.RootFS(ctx)
	if err != nil {
		return nil, nil, err
	}
	rootfs := image.NewRootFS()
	for _, id := range fs {
		rootfs.Append(layer.DiffID(id))
	}
	exposedPorts := make(nat.PortSet, len(ociimage.Config.ExposedPorts))
	for k, v := range ociimage.Config.ExposedPorts {
		exposedPorts[nat.Port(k)] = v
	}

	img := image.NewImage(image.IDFromDigest(ctrdimg.Target.Digest))
	img.V1Image = image.V1Image{
		ID:           string(ctrdimg.Target.Digest),
		OS:           ociimage.OS,
		Architecture: ociimage.Architecture,
		Config: &containertypes.Config{
			Entrypoint:   ociimage.Config.Entrypoint,
			Env:          ociimage.Config.Env,
			Cmd:          ociimage.Config.Cmd,
			User:         ociimage.Config.User,
			WorkingDir:   ociimage.Config.WorkingDir,
			ExposedPorts: exposedPorts,
			Volumes:      ociimage.Config.Volumes,
		},
	}
	img.RootFS = rootfs

	return containerdImage, img, nil
}

// resolveImage searches for an image based on the given
// reference or identifier. Returns the descriptor of
// the image, which could be a manifest list, manifest, or config.
func (i *ImageService) resolveImage(ctx context.Context, refOrID string, platform *ocispec.Platform) (img containerdimages.Image, err error) {
	parsed, err := reference.ParseAnyReference(refOrID)
	if err != nil {
		return containerdimages.Image{}, errdefs.InvalidParameter(err)
	}

	is := i.client.ImageService()

	digested, ok := parsed.(reference.Digested)
	if ok {
		imgs, err := is.List(ctx, "target.digest=="+digested.Digest().String())
		if err != nil {
			return containerdimages.Image{}, errors.Wrap(err, "failed to lookup digest")
		}
		if len(imgs) == 0 {
			return containerdimages.Image{}, images.ErrImageDoesNotExist{Ref: parsed}
		}

		return imgs[0], nil
	}

	ref := reference.TagNameOnly(parsed.(reference.Named)).String()

	// If the identifier could be a short ID, attempt to match
	if truncatedID.MatchString(refOrID) {
		filters := []string{
			fmt.Sprintf("name==%q", ref), // Or it could just look like one.
			"target.digest~=" + strconv.Quote(fmt.Sprintf(`sha256:^%s[0-9a-fA-F]{%d}$`, regexp.QuoteMeta(refOrID), 64-len(refOrID))),
		}
		imgs, err := is.List(ctx, filters...)
		if err != nil {
			return containerdimages.Image{}, err
		}

		if len(imgs) == 0 {
			return containerdimages.Image{}, images.ErrImageDoesNotExist{Ref: parsed}
		}
		if len(imgs) > 1 {
			digests := map[digest.Digest]struct{}{}
			for _, img := range imgs {
				if img.Name == ref {
					return img, nil
				}
				digests[img.Target.Digest] = struct{}{}
			}

			if len(digests) > 1 {
				return containerdimages.Image{}, errdefs.NotFound(errors.New("ambiguous reference"))
			}
		}

		return imgs[0], nil
	}

	img, err = is.Get(ctx, ref)
	if err != nil {
		// TODO(containerd): error translation can use common function
		if !cerrdefs.IsNotFound(err) {
			return containerdimages.Image{}, err
		}
		return containerdimages.Image{}, images.ErrImageDoesNotExist{Ref: parsed}
	}
	if platform != nil {
		cs := i.client.ContentStore()
		imgPlatforms, err := containerdimages.Platforms(ctx, cs, img.Target)
		if err != nil {
			return img, err
		}

		comparer := platforms.Only(*platform)
		for _, p := range imgPlatforms {
			if comparer.Match(p) {
				return img, nil
			}
		}
		return img, errdefs.NotFound(errors.Errorf("%s: platform %s not supported by image", refOrID, platforms.Format(*platform)))
	}
	return img, nil
}
