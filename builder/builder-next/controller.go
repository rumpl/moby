package buildkit

import (
	"context"
	registryremotecache "github.com/moby/buildkit/cache/remotecache/registry"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/solver/bboltcachestorage"
	"github.com/moby/buildkit/util/network/cniprovider"
	"github.com/moby/buildkit/util/network/netproviders"
	"github.com/moby/buildkit/worker/base"
	"github.com/moby/buildkit/worker/containerd"
	"net/http"
	"os"
	"path/filepath"
	"time"

	ctd "github.com/containerd/containerd"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	mobyworker "github.com/docker/docker/builder/builder-next/worker"
	"github.com/docker/docker/daemon/config"
	units "github.com/docker/go-units"
	"github.com/moby/buildkit/cache/remotecache"
	inlineremotecache "github.com/moby/buildkit/cache/remotecache/inline"
	localremotecache "github.com/moby/buildkit/cache/remotecache/local"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/control"
	"github.com/moby/buildkit/frontend"
	dockerfile "github.com/moby/buildkit/frontend/dockerfile/builder"
	"github.com/moby/buildkit/frontend/gateway"
	"github.com/moby/buildkit/frontend/gateway/forwarder"
	"github.com/moby/buildkit/util/entitlements"
	"github.com/moby/buildkit/worker"
	"github.com/pkg/errors"
)

func newController(rt http.RoundTripper, opt Opt) (*control.Controller, error) {
	if err := os.MkdirAll(opt.Root, 0711); err != nil {
		return nil, err
	}

	nc := netproviders.Opt{
		Mode: "auto",
		CNI: cniprovider.Opt{
			Root:       opt.Root,
			ConfigPath: "/etc/buildkit/cni.json",
			BinaryDir:  "/opt/cni/bin",
		},
	}

	dns := getDNSConfig(opt.DNSConfig)

	snapshotter := ctd.DefaultSnapshotter
	wo, err := containerd.NewWorkerOpt(opt.Root, opt.Config.ContainerdAddr, snapshotter, opt.Config.ContainerdNamespace,
		false, map[string]string{}, dns, nc, opt.ApparmorProfile, nil, "", ctd.WithTimeout(60*time.Second))
	if err != nil {
		return nil, err
	}
	policy, err := getGCPolicy(opt.BuilderConfig, opt.Root)
	if err != nil {
		return nil, err
	}
	wo.GCPolicy = policy

	wo.ImageStore = opt.ContainerdCli.ImageService()
	wo.ContentStore = opt.ContainerdCli.ContentStore()
	wo.LeaseManager = opt.ContainerdCli.LeasesService()
	wo.Snapshotter = snapshot.FromContainerdSnapshotter(ctd.DefaultSnapshotter, opt.ContainerdCli.SnapshotService(ctd.DefaultSnapshotter), &opt.IdentityMapping)
	wo.Applier = opt.ContainerdCli.DiffService()
	wo.Differ = opt.ContainerdCli.DiffService()
	wo.RegistryHosts = opt.RegistryHosts

	w, err := base.NewWorker(context.TODO(), wo)
	if err != nil {
		return nil, err
	}
	wc := &worker.Controller{}
	err = wc.Add(w)
	if err != nil {
		return nil, err
	}

	frontends := map[string]frontend.Frontend{
		"dockerfile.v0": forwarder.NewGatewayForwarder(wc, dockerfile.Build),
		"gateway.v0":    gateway.NewGatewayFrontend(wc),
	}

	cacheStorage, err := bboltcachestorage.NewStore(filepath.Join(opt.Root, "cache.db"))
	if err != nil {
		return nil, err
	}

	return control.NewController(control.Opt{
		SessionManager:   opt.SessionManager,
		WorkerController: wc,
		Frontends:        frontends,
		CacheKeyStorage:  cacheStorage,
		ResolveCacheImporterFuncs: map[string]remotecache.ResolveCacheImporterFunc{
			"registry": registryremotecache.ResolveCacheImporterFunc(opt.SessionManager, w.ContentStore(), opt.RegistryHosts),
			"local":    localremotecache.ResolveCacheImporterFunc(opt.SessionManager),
		},
		ResolveCacheExporterFuncs: map[string]remotecache.ResolveCacheExporterFunc{
			"registry": registryremotecache.ResolveCacheExporterFunc(opt.SessionManager, opt.RegistryHosts),
			"local":    localremotecache.ResolveCacheExporterFunc(opt.SessionManager),
			"inline":   inlineremotecache.ResolveCacheExporterFunc(),
		},
		Entitlements: getEntitlements(opt.BuilderConfig),
	})
}

func getGCPolicy(conf config.BuilderConfig, root string) ([]client.PruneInfo, error) {
	var gcPolicy []client.PruneInfo
	if conf.GC.Enabled {
		var (
			defaultKeepStorage int64
			err                error
		)

		if conf.GC.DefaultKeepStorage != "" {
			defaultKeepStorage, err = units.RAMInBytes(conf.GC.DefaultKeepStorage)
			if err != nil {
				return nil, errors.Wrapf(err, "could not parse '%s' as Builder.GC.DefaultKeepStorage config", conf.GC.DefaultKeepStorage)
			}
		}

		if conf.GC.Policy == nil {
			gcPolicy = mobyworker.DefaultGCPolicy(root, defaultKeepStorage)
		} else {
			gcPolicy = make([]client.PruneInfo, len(conf.GC.Policy))
			for i, p := range conf.GC.Policy {
				b, err := units.RAMInBytes(p.KeepStorage)
				if err != nil {
					return nil, err
				}
				if b == 0 {
					b = defaultKeepStorage
				}
				gcPolicy[i], err = toBuildkitPruneInfo(types.BuildCachePruneOptions{
					All:         p.All,
					KeepStorage: b,
					Filters:     filters.Args(p.Filter),
				})
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return gcPolicy, nil
}

func getEntitlements(conf config.BuilderConfig) []string {
	var ents []string
	// Incase of no config settings, NetworkHost should be enabled & SecurityInsecure must be disabled.
	if conf.Entitlements.NetworkHost == nil || *conf.Entitlements.NetworkHost {
		ents = append(ents, string(entitlements.EntitlementNetworkHost))
	}
	if conf.Entitlements.SecurityInsecure != nil && *conf.Entitlements.SecurityInsecure {
		ents = append(ents, string(entitlements.EntitlementSecurityInsecure))
	}
	return ents
}
