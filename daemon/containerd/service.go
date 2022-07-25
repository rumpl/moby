package containerd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/images/archive"
	"github.com/containerd/containerd/images/converter"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/backend"
	containertypes "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	imagetype "github.com/docker/docker/api/types/image"
	registrytypes "github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/builder"
	"github.com/docker/docker/container"
	"github.com/docker/docker/daemon/images"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/image"
	"github.com/docker/docker/layer"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var shortID = regexp.MustCompile(`^([a-f0-9]{4,64})$`)

type containerdStore struct {
	client *containerd.Client
}

func NewService(c *containerd.Client) *containerdStore {
	return &containerdStore{
		client: c,
	}
}

func (cs *containerdStore) PullImage(ctx context.Context, image, tag string, platform *ocispec.Platform, metaHeaders map[string][]string, authConfig *types.AuthConfig, outStream io.Writer) error {
	var opts []containerd.RemoteOpt
	if platform != nil {
		opts = append(opts, containerd.WithPlatform(platforms.Format(*platform)))
	}
	ref, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return errdefs.InvalidParameter(err)
	}

	if tag != "" {
		// The "tag" could actually be a digest.
		var dgst digest.Digest
		dgst, err = digest.Parse(tag)
		if err == nil {
			ref, err = reference.WithDigest(reference.TrimNamed(ref), dgst)
		} else {
			ref, err = reference.WithTag(ref, tag)
		}
		if err != nil {
			return errdefs.InvalidParameter(err)
		}
	}

	resolver, _ := newResolverFromAuthConfig(authConfig)
	opts = append(opts, containerd.WithResolver(resolver))

	jobs := newJobs()
	h := containerdimages.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		if desc.MediaType != containerdimages.MediaTypeDockerSchema1Manifest {
			jobs.Add(desc)
		}
		return nil, nil
	})
	opts = append(opts, containerd.WithImageHandler(h))

	finishProgress := showProgress(ctx, jobs, outStream, pullProgress(cs.client.ContentStore()))
	defer finishProgress()

	img, err := cs.client.Pull(ctx, ref.String(), opts...)
	if err != nil {
		return err
	}

	unpacked, err := img.IsUnpacked(ctx, containerd.DefaultSnapshotter)
	if err != nil {
		return err
	}

	if !unpacked {
		if err := img.Unpack(ctx, containerd.DefaultSnapshotter); err != nil {
			return err
		}
	}
	return err
}

type imageFilterFunc func(image containerd.Image) bool

func (cs *containerdStore) Images(ctx context.Context, opts types.ImageListOptions) ([]*types.ImageSummary, error) {
	images, err := cs.client.ListImages(ctx)
	if err != nil {
		return nil, err
	}

	filters, err := cs.setupFilters(ctx, opts)
	if err != nil {
		return nil, err
	}

	snapshotter := cs.client.SnapshotService(containerd.DefaultSnapshotter)
	sizeCache := make(map[digest.Digest]int64)
	snapshotSizeFn := func(d digest.Digest) (int64, error) {
		if s, ok := sizeCache[d]; ok {
			return s, nil
		}
		usage, err := snapshotter.Usage(ctx, d.String())
		if err != nil {
			return 0, err
		}
		sizeCache[d] = usage.Size
		return usage.Size, nil
	}

	ret := make([]*types.ImageSummary, 0, len(images))
	var (
		root   []*[]digest.Digest
		layers map[digest.Digest]int
	)
	if opts.SharedSize {
		root = make([]*[]digest.Digest, len(images))
		layers = make(map[digest.Digest]int)
	}
IMAGES:
	for i, image := range images {
		for _, filter := range filters {
			if !filter(image) {
				continue IMAGES
			}
		}

		diffIDs, err := image.RootFS(ctx)
		if err != nil {
			return nil, err
		}
		chainIDs := identity.ChainIDs(diffIDs)
		if opts.SharedSize {
			root[i] = &chainIDs
			for _, id := range chainIDs {
				layers[id] = layers[id] + 1
			}
		}

		size, err := image.Size(ctx)
		if err != nil {
			return nil, err
		}

		virtualSize, err := computeVirtualSize(chainIDs, snapshotSizeFn)
		if err != nil {
			return nil, err
		}

		ret = append(ret, &types.ImageSummary{
			RepoDigests: []string{image.Name() + "@" + image.Target().Digest.String()}, // "hello-world@sha256:bfea6278a0a267fad2634554f4f0c6f31981eea41c553fdf5a83e95a41d40c38"},
			RepoTags:    []string{image.Name()},
			Containers:  -1,
			ParentID:    "",
			SharedSize:  -1,
			VirtualSize: virtualSize,
			ID:          image.Target().Digest.String(),
			Created:     image.Metadata().CreatedAt.Unix(),
			Size:        size,
		})
	}

	if opts.SharedSize {
		for i, chainIDs := range root {
			sharedSize, err := computeSharedSize(*chainIDs, layers, snapshotSizeFn)
			if err != nil {
				return nil, err
			}
			ret[i].SharedSize = sharedSize
		}
	}

	return ret, nil
}

func computeVirtualSize(chainIDs []digest.Digest, sizeFn func(d digest.Digest) (int64, error)) (int64, error) {
	var virtualSize int64
	for _, chainID := range chainIDs {
		size, err := sizeFn(chainID)
		if err != nil {
			return virtualSize, err
		}
		virtualSize += size
	}
	return virtualSize, nil
}

func computeSharedSize(chainIDs []digest.Digest, layers map[digest.Digest]int, sizeFn func(d digest.Digest) (int64, error)) (int64, error) {
	var sharedSize int64
	for _, chainID := range chainIDs {
		if layers[chainID] == 1 {
			continue
		}
		size, err := sizeFn(chainID)
		if err != nil {
			return 0, err
		}
		sharedSize += size
	}
	return sharedSize, nil
}

func (cs *containerdStore) setupFilters(ctx context.Context, opts types.ImageListOptions) ([]imageFilterFunc, error) {
	var filters []imageFilterFunc
	err := opts.Filters.WalkValues("before", func(value string) error {
		ref, err := reference.ParseDockerRef(value)
		if err != nil {
			return err
		}
		img, err := cs.client.GetImage(ctx, ref.String())
		if img != nil {
			t := img.Metadata().CreatedAt
			filters = append(filters, func(image containerd.Image) bool {
				created := image.Metadata().CreatedAt
				return created.Equal(t) || created.After(t)
			})
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	err = opts.Filters.WalkValues("since", func(value string) error {
		ref, err := reference.ParseDockerRef(value)
		if err != nil {
			return err
		}
		img, err := cs.client.GetImage(ctx, ref.String())
		if img != nil {
			t := img.Metadata().CreatedAt
			filters = append(filters, func(image containerd.Image) bool {
				created := image.Metadata().CreatedAt
				return created.Equal(t) || created.Before(t)
			})
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	if opts.Filters.Contains("label") {
		filters = append(filters, func(image containerd.Image) bool {
			return opts.Filters.MatchKVList("label", image.Labels())
		})
	}
	return filters, nil
}

func newResolverFromAuthConfig(authConfig *types.AuthConfig) (remotes.Resolver, docker.StatusTracker) {
	opts := []docker.RegistryOpt{}

	if authConfig != nil {
		authorizer := docker.NewDockerAuthorizer(docker.WithAuthCreds(func(_ string) (string, string, error) {
			if authConfig.IdentityToken != "" {
				return "", authConfig.IdentityToken, nil
			}
			return authConfig.Username, authConfig.Password, nil
		}))

		opts = append(opts, docker.WithAuthorizer(authorizer))
	}

	tracker := docker.NewInMemoryTracker()

	return docker.NewResolver(docker.ResolverOptions{
		Hosts:   docker.ConfigureDefaultRegistries(opts...),
		Tracker: tracker,
	}), tracker
}

func (cs *containerdStore) LogImageEvent(imageID, refName, action string) {
	panic("not implemented")
}

func (cs *containerdStore) LogImageEventWithAttributes(imageID, refName, action string, attributes map[string]string) {
	panic("not implemented")
}

func (cs *containerdStore) GetLayerFolders(img *image.Image, rwLayer layer.RWLayer) ([]string, error) {
	panic("not implemented")
}

func (cs *containerdStore) Map() map[image.ID]*image.Image {
	panic("not implemented")
}

func (cs *containerdStore) GetLayerByID(string) (layer.RWLayer, error) {
	panic("not implemented")
}

func (cs *containerdStore) GetLayerMountID(string) (string, error) {
	panic("not implemented")
}

func (cs *containerdStore) Cleanup() error {
	return nil
}

func (cs *containerdStore) GraphDriverName() string {
	return "containerd-snapshotter"
}

func (cs *containerdStore) CommitBuildStep(c backend.CommitConfig) (image.ID, error) {
	panic("not implemented")
}

func (cs *containerdStore) CreateImage(config []byte, parent string) (builder.Image, error) {
	panic("not implemented")
}

func (cs *containerdStore) GetImageAndReleasableLayer(ctx context.Context, refOrID string, opts backend.GetImageAndLayerOptions) (builder.Image, builder.ROLayer, error) {
	panic("not implemented")
}

func (cs *containerdStore) MakeImageCache(ctx context.Context, cacheFrom []string) builder.ImageCache {
	panic("not implemented")
}

func (cs *containerdStore) TagImageWithReference(ctx context.Context, imageID image.ID, newTag reference.Named) error {
	logrus.Infof("Tagging image %q with reference %q", imageID, newTag.String())

	desc, err := cs.ResolveImage(ctx, imageID.String())
	if err != nil {
		return err
	}

	img := containerdimages.Image{
		Name:   newTag.String(),
		Target: desc,
	}

	is := cs.client.ImageService()
	_, err = is.Create(ctx, img)

	return err
}

func (cs *containerdStore) SquashImage(id, parent string) (string, error) {
	panic("not implemented")
}

func (cs *containerdStore) ExportImage(ctx context.Context, names []string, outStream io.Writer) error {
	opts := []archive.ExportOpt{
		archive.WithPlatform(platforms.Ordered(platforms.DefaultSpec())),
		archive.WithSkipNonDistributableBlobs(),
	}
	is := cs.client.ImageService()
	for _, imageRef := range names {
		named, err := reference.ParseDockerRef(imageRef)
		if err != nil {
			return err
		}
		opts = append(opts, archive.WithImage(is, named.String()))
	}
	return cs.client.Export(ctx, outStream, opts...)
}

func (cs *containerdStore) ImageDelete(ctx context.Context, imageRef string, force, prune bool) ([]types.ImageDeleteResponseItem, error) {
	records := []types.ImageDeleteResponseItem{}

	parsedRef, err := reference.ParseNormalizedNamed(imageRef)
	if err != nil {
		return nil, err
	}
	ref := reference.TagNameOnly(parsedRef)

	if err := cs.client.ImageService().Delete(ctx, ref.String(), containerdimages.SynchronousDelete()); err != nil {
		return []types.ImageDeleteResponseItem{}, err
	}

	d := types.ImageDeleteResponseItem{Untagged: reference.FamiliarString(parsedRef)}
	records = append(records, d)

	return records, nil
}

func (cs *containerdStore) ImageHistory(name string) ([]*imagetype.HistoryResponseItem, error) {
	panic("not implemented")
}

func (cs *containerdStore) ImportImage(ctx context.Context, src string, repository string, platform *v1.Platform, tag string, msg string, inConfig io.ReadCloser, outStream io.Writer, changes []string) error {
	panic("not implemented")
}

func (cs *containerdStore) LoadImage(ctx context.Context, inTar io.ReadCloser, outStream io.Writer, quiet bool) error {
	platform := platforms.DefaultStrict()
	imgs, err := cs.client.Import(ctx, inTar, containerd.WithImportPlatform(platform))

	if err != nil {
		logrus.WithError(err).Error("Failed to import image to containerd")
		return errors.Wrapf(err, "Failed to import image")
	}

	for _, img := range imgs {
		platformImg := containerd.NewImageWithPlatform(cs.client, img, platform)

		unpacked, err := platformImg.IsUnpacked(ctx, containerd.DefaultSnapshotter)
		if err != nil {
			logrus.WithError(err).WithField("image", img.Name).Error("IsUnpacked failed")
			continue
		}

		if !unpacked {
			err := platformImg.Unpack(ctx, containerd.DefaultSnapshotter)
			if err != nil {
				logrus.WithError(err).WithField("image", img.Name).Error("Failed to unpack image")
				return errors.Wrapf(err, "Failed to unpack image")
			}
		}
	}
	return nil
}

func (cs *containerdStore) LookupImage(ctx context.Context, name string) (*types.ImageInspect, error) {
	panic("not implemented")
}

func (cs *containerdStore) PushImage(ctx context.Context, image, tag string, metaHeaders map[string][]string, authConfig *types.AuthConfig, outStream io.Writer) error {
	// TODO: Pass this from user?
	platformMatcher := platforms.DefaultStrict()

	ref, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return err
	}
	if tag != "" {
		// Push by digest is not supported, so only tags are supported.
		ref, err = reference.WithTag(ref, tag)
		if err != nil {
			return err
		}
	}

	is := cs.client.ImageService()
	store := cs.client.ContentStore()

	img, err := is.Get(ctx, ref.String())
	if err != nil {
		return errors.Wrap(err, "Failed to get image")
	}

	target := img.Target

	// Create a temporary image which is stripped from content that references other platforms.
	// We or the remote may not have them and referencing them will end with an error.
	if platformMatcher != platforms.All {
		tmpRef := ref.String() + "-tmp-platformspecific"
		platformImg, err := converter.Convert(ctx, cs.client, tmpRef, ref.String(), converter.WithPlatform(platformMatcher))
		if err != nil {
			return errors.Wrap(err, "Failed to convert image to platform specific")
		}

		target = platformImg.Target
		defer cs.client.ImageService().Delete(ctx, platformImg.Name, containerdimages.SynchronousDelete())
	}

	jobs := newJobs()

	imageHandler := containerdimages.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) (subdescs []ocispec.Descriptor, err error) {
		logrus.WithField("desc", desc).Debug("Pushing")
		if desc.MediaType != containerdimages.MediaTypeDockerSchema1Manifest {
			children, err := containerdimages.Children(ctx, store, desc)

			if err == nil {
				for _, c := range children {
					jobs.Add(c)
				}
			}

			jobs.Add(desc)
		}
		return nil, nil
	})
	imageHandler = remotes.SkipNonDistributableBlobs(imageHandler)

	resolver, tracker := newResolverFromAuthConfig(authConfig)

	finishProgress := showProgress(ctx, jobs, outStream, pushProgress(tracker))
	defer finishProgress()

	logrus.WithField("desc", target).WithField("ref", ref.String()).Info("Pushing desc to remote ref")
	err = cs.client.Push(ctx, ref.String(), target,
		containerd.WithResolver(resolver),
		containerd.WithPlatformMatcher(platformMatcher),
		containerd.WithImageHandler(imageHandler),
	)

	return err
}

func (cs *containerdStore) SearchRegistryForImages(ctx context.Context, searchFilters filters.Args, term string, limit int, authConfig *types.AuthConfig, metaHeaders map[string][]string) (*registrytypes.SearchResults, error) {
	panic("not implemented")
}

func (cs *containerdStore) TagImage(ctx context.Context, imageName, repository, tag string) (string, error) {
	desc, err := cs.ResolveImage(ctx, imageName)
	if err != nil {
		return "", err
	}

	newTag, err := reference.ParseNormalizedNamed(repository)
	if err != nil {
		return "", err
	}
	if tag != "" {
		if newTag, err = reference.WithTag(reference.TrimNamed(newTag), tag); err != nil {
			return "", err
		}
	}

	err = cs.TagImageWithReference(ctx, image.ID(desc.Digest), newTag)
	return reference.FamiliarString(newTag), err
}

func (cs *containerdStore) GetRepository(context.Context, reference.Named, *types.AuthConfig) (distribution.Repository, error) {
	panic("not implemented")
}

func (cs *containerdStore) ImageDiskUsage(ctx context.Context) ([]*types.ImageSummary, error) {
	panic("not implemented")
}

func (cs *containerdStore) LayerDiskUsage(ctx context.Context) (int64, error) {
	panic("not implemented")
}

func (cs *containerdStore) ReleaseLayer(rwlayer layer.RWLayer) error {
	panic("not implemented")
}

func (cs *containerdStore) CommitImage(c backend.CommitConfig) (image.ID, error) {
	panic("not implemented")
}

func (cs *containerdStore) GetImage(ctx context.Context, refOrID string, platform *v1.Platform) (*image.Image, error) {
	desc, err := cs.ResolveImage(ctx, refOrID)
	if err != nil {
		return nil, err
	}

	ctrdimg, err := cs.resolveImageName2(ctx, refOrID)
	if err != nil {
		return nil, err
	}
	ii := containerd.NewImage(cs.client, ctrdimg)
	provider := cs.client.ContentStore()
	conf, err := ctrdimg.Config(ctx, provider, ii.Platform())
	if err != nil {
		return nil, err
	}

	var ociimage v1.Image
	imageConfigBytes, err := content.ReadBlob(ctx, ii.ContentStore(), conf)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(imageConfigBytes, &ociimage); err != nil {
		return nil, err
	}

	return &image.Image{
		V1Image: image.V1Image{
			ID:           string(desc.Digest),
			OS:           ociimage.OS,
			Architecture: ociimage.Architecture,
			Config: &containertypes.Config{
				Entrypoint: ociimage.Config.Entrypoint,
				Env:        ociimage.Config.Env,
				Cmd:        ociimage.Config.Cmd,
				User:       ociimage.Config.User,
				WorkingDir: ociimage.Config.WorkingDir,
			},
		},
	}, nil
}

func (cs *containerdStore) CreateLayer(container *container.Container, initFunc layer.MountInit) (layer.RWLayer, error) {
	panic("not implemented")
}

func (cs *containerdStore) DistributionServices() images.DistributionServices {
	return images.DistributionServices{}
}

func (cs *containerdStore) CountImages() int {
	imgs, err := cs.client.ListImages(context.TODO())
	if err != nil {
		return 0
	}

	return len(imgs)
}

func (cs *containerdStore) LayerStoreStatus() [][2]string {
	return [][2]string{}
}

func (cs *containerdStore) GetContainerLayerSize(ctx context.Context, containerID string) (int64, int64, error) {
	snapshotter := cs.client.SnapshotService(containerd.DefaultSnapshotter)
	sizeCache := make(map[digest.Digest]int64)
	snapshotSizeFn := func(d digest.Digest) (int64, error) {
		if s, ok := sizeCache[d]; ok {
			return s, nil
		}
		usage, err := snapshotter.Usage(ctx, d.String())
		if err != nil {
			return 0, err
		}
		sizeCache[d] = usage.Size
		return usage.Size, nil
	}

	c, err := cs.client.ContainerService().Get(ctx, containerID)
	if err != nil {
		return 0, 0, err
	}
	image, err := cs.client.GetImage(ctx, c.Image)
	if err != nil {
		return 0, 0, err
	}
	diffIDs, err := image.RootFS(ctx)
	if err != nil {
		return 0, 0, err
	}
	chainIDs := identity.ChainIDs(diffIDs)

	usage, err := snapshotter.Usage(ctx, containerID)
	if err != nil {
		return 0, 0, err
	}
	size := usage.Size

	virtualSize, err := computeVirtualSize(chainIDs, snapshotSizeFn)
	if err != nil {
		return 0, 0, err
	}
	return size, size + virtualSize, nil
}

func (cs *containerdStore) UpdateConfig(maxDownloads, maxUploads int) {
	panic("not implemented")
}

func (cs *containerdStore) Children(id image.ID) []image.ID {
	panic("not implemented")
}

// ResolveImage searches for an image based on the given
// reference or identifier. Returns the descriptor of
// the image, could be manifest list, manifest, or config.
func (cs *containerdStore) ResolveImage(ctx context.Context, refOrID string) (d ocispec.Descriptor, err error) {
	d, _, err = cs.resolveImageName(ctx, refOrID)
	return
}

func (cs *containerdStore) resolveImageName2(ctx context.Context, refOrID string) (img containerdimages.Image, err error) {
	parsed, err := reference.ParseAnyReference(refOrID)
	if err != nil {
		return img, errdefs.InvalidParameter(err)
	}

	is := cs.client.ImageService()

	namedRef, ok := parsed.(reference.Named)
	if !ok {
		digested, ok := parsed.(reference.Digested)
		if !ok {
			return img, errdefs.InvalidParameter(errors.New("bad reference"))
		}

		imgs, err := is.List(ctx, fmt.Sprintf("target.digest==%s", digested.Digest()))
		if err != nil {
			return img, errors.Wrap(err, "failed to lookup digest")
		}
		if len(imgs) == 0 {
			return img, errdefs.NotFound(errors.New("image not found with digest"))
		}

		return imgs[0], nil
	}

	namedRef = reference.TagNameOnly(namedRef)

	// If the identifier could be a short ID, attempt to match
	if shortID.MatchString(refOrID) {
		ref := namedRef.String()
		filters := []string{
			fmt.Sprintf("name==%q", ref),
			fmt.Sprintf(`target.digest~=/sha256:%s[0-9a-fA-F]{%d}/`, refOrID, 64-len(refOrID)),
		}
		imgs, err := is.List(ctx, filters...)
		if err != nil {
			return img, err
		}

		if len(imgs) == 0 {
			return img, errdefs.NotFound(errors.New("list returned no images"))
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
				return img, errdefs.NotFound(errors.New("ambiguous reference"))
			}
		}

		if imgs[0].Name != ref {
			namedRef = nil
		}
		return imgs[0], nil
	}
	img, err = is.Get(ctx, namedRef.String())
	if err != nil {
		// TODO(containerd): error translation can use common function
		if !cerrdefs.IsNotFound(err) {
			return img, err
		}
		return img, errdefs.NotFound(errors.New("id not found"))
	}

	return img, nil
}

func (cs *containerdStore) resolveImageName(ctx context.Context, refOrID string) (ocispec.Descriptor, reference.Named, error) {
	parsed, err := reference.ParseAnyReference(refOrID)
	if err != nil {
		return ocispec.Descriptor{}, nil, errdefs.InvalidParameter(err)
	}

	is := cs.client.ImageService()

	namedRef, ok := parsed.(reference.Named)
	if !ok {
		digested, ok := parsed.(reference.Digested)
		if !ok {
			return ocispec.Descriptor{}, nil, errdefs.InvalidParameter(errors.New("bad reference"))
		}

		imgs, err := is.List(ctx, fmt.Sprintf("target.digest==%s", digested.Digest()))
		if err != nil {
			return ocispec.Descriptor{}, nil, errors.Wrap(err, "failed to lookup digest")
		}
		if len(imgs) == 0 {
			return ocispec.Descriptor{}, nil, errdefs.NotFound(errors.New("image not found with digest"))
		}

		return imgs[0].Target, nil, nil
	}

	namedRef = reference.TagNameOnly(namedRef)

	// If the identifier could be a short ID, attempt to match
	if shortID.MatchString(refOrID) {
		ref := namedRef.String()
		filters := []string{
			fmt.Sprintf("name==%q", ref),
			fmt.Sprintf(`target.digest~=/sha256:%s[0-9a-fA-F]{%d}/`, refOrID, 64-len(refOrID)),
		}
		imgs, err := is.List(ctx, filters...)
		if err != nil {
			return ocispec.Descriptor{}, nil, err
		}

		if len(imgs) == 0 {
			return ocispec.Descriptor{}, nil, errdefs.NotFound(errors.New("list returned no images"))
		}
		if len(imgs) > 1 {
			digests := map[digest.Digest]struct{}{}
			for _, img := range imgs {
				if img.Name == ref {
					return img.Target, namedRef, nil
				}
				digests[img.Target.Digest] = struct{}{}
			}

			if len(digests) > 1 {
				return ocispec.Descriptor{}, nil, errdefs.NotFound(errors.New("ambiguous reference"))
			}
		}

		if imgs[0].Name != ref {
			namedRef = nil
		}
		return imgs[0].Target, namedRef, nil
	}
	img, err := is.Get(ctx, namedRef.String())
	if err != nil {
		// TODO(containerd): error translation can use common function
		if !cerrdefs.IsNotFound(err) {
			return ocispec.Descriptor{}, nil, err
		}
		return ocispec.Descriptor{}, nil, errdefs.NotFound(errors.New("id not found"))
	}

	return img.Target, namedRef, nil
}
