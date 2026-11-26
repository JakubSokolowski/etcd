package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/savescum"
)

type memberClient struct {
	name     string
	endpoint string
	cli      *clientv3.Client
}

// SwarmProfile implements Groce et al. (ISSTA 2012) Swarm Testing:
// each branch draws a distinct subset of features, concurrency, and pacing
// from deterministic /dev/urandom instead of running every feature uniformly.
type SwarmProfile struct {
	WriteWorkers        int
	WritePaceMs         int
	KeySpace            int
	EnableDeletes       bool
	EnableCompaction    bool
	EnableLinReads      bool
	ReadWorkers         int
	EnableFutureWatches bool
}

func sampleSwarmProfile() SwarmProfile {
	writeWorkerChoices := []int{1, 2, 4, 8}
	writePaceChoices := []int{0, 2, 8, 25}
	keySpaceChoices := []int{2, 8, 32}
	readWorkerChoices := []int{1, 4}

	p := SwarmProfile{
		WriteWorkers:        writeWorkerChoices[savescum.RandomU32()%uint32(len(writeWorkerChoices))],
		WritePaceMs:         writePaceChoices[savescum.RandomU32()%uint32(len(writePaceChoices))],
		KeySpace:            keySpaceChoices[savescum.RandomU32()%uint32(len(keySpaceChoices))],
		EnableDeletes:       (savescum.RandomU32() & 1) == 1,
		EnableCompaction:    (savescum.RandomU32() & 1) == 1,
		EnableLinReads:      (savescum.RandomU32() & 1) == 1,
		ReadWorkers:         readWorkerChoices[savescum.RandomU32()%uint32(len(readWorkerChoices))],
		EnableFutureWatches: (savescum.RandomU32() & 1) == 1,
	}
	return p
}

func (p SwarmProfile) Tag() string {
	b := func(v bool) int {
		if v {
			return 1
		}
		return 0
	}
	return fmt.Sprintf("w%d_p%d_k%d_d%d_c%d_r%d_f%d",
		p.WriteWorkers, p.WritePaceMs, p.KeySpace,
		b(p.EnableDeletes), b(p.EnableCompaction), b(p.EnableLinReads), b(p.EnableFutureWatches))
}

func parseMembers() []string {
	raw := strings.TrimSpace(os.Getenv("ETCD_ENDPOINTS"))
	if raw == "" {
		raw = "http://etcd1:2379,http://etcd2:2379,http://etcd3:2379"
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func memberName(endpoint string, idx int) string {
	trimmed := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	if host, _, ok := strings.Cut(trimmed, ":"); ok && host != "" {
		return host
	}
	return fmt.Sprintf("etcd%d", idx+1)
}

func dialEndpoints(endpoints []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          1 * time.Second,
		DialKeepAliveTime:    5 * time.Second,
		DialKeepAliveTimeout: 2 * time.Second,
		AutoSyncInterval:     0,
	})
}

func waitForCluster(members []*memberClient, quorumCli *clientv3.Client) int64 {
	log.Printf("driver: waiting for %d etcd members to form quorum", len(members))
	for attempt := 1; attempt <= 400; attempt++ {
		allReady := true
		leaderSeen := false
		for _, m := range members {
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			st, err := m.cli.Status(ctx, m.endpoint)
			cancel()
			if err != nil {
				allReady = false
				break
			}
			if st.Leader != 0 {
				leaderSeen = true
			}
		}
		if allReady && leaderSeen {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			resp, err := quorumCli.Put(ctx, "k/init", "ready")
			cancel()
			if err == nil && resp != nil && resp.Header != nil {
				log.Printf("driver: quorum ready after attempt=%d initRev=%d", attempt, resp.Header.Revision)
				return resp.Header.Revision
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Fatalf("driver: timed out waiting for etcd cluster quorum")
	return 0
}

func runPersistentWatchVerifier(
	m *memberClient,
	initialStartRev int64,
	warmupRev int64,
	warmupReady chan<- struct{},
	maxCommittedRev *atomic.Int64,
) {
	startRev := initialStartRev
	var lastEventModRev int64
	warmupSignaled := false
	reconnects := 0

	for {
		ctx, cancel := context.WithCancel(context.Background())
		wch := m.cli.Watch(
			ctx,
			"k/",
			clientv3.WithPrefix(),
			clientv3.WithRev(startRev),
			clientv3.WithCreatedNotify(),
		)

		for resp := range wch {
			if err := resp.Err(); err != nil {
				break
			}
			for _, ev := range resp.Events {
				modRev := ev.Kv.ModRevision
				key := string(ev.Kv.Key)

				if modRev < startRev {
					log.Printf("driver: BUG watch_start_rev_bound member=%s key=%s startRev=%d gotModRev=%d headerRev=%d",
						m.name, key, startRev, modRev, resp.Header.Revision)
					savescum.Always("etcd_watch_start_rev_bound", false, map[string]any{
						"member":        m.name,
						"key":           key,
						"watchStartRev": startRev,
						"gotModRev":     modRev,
						"respHeaderRev": resp.Header.Revision,
						"watchKind":     "persistent",
					})
				} else {
					savescum.Always("etcd_watch_start_rev_bound", true, nil)
				}

				if lastEventModRev > 0 && modRev <= lastEventModRev {
					log.Printf("driver: BUG watch_revision_monotonic member=%s key=%s lastEventModRev=%d gotModRev=%d delta=%d headerRev=%d startRev=%d",
						m.name, key, lastEventModRev, modRev, lastEventModRev-modRev, resp.Header.Revision, startRev)
					savescum.Always("etcd_watch_revision_monotonic", false, map[string]any{
						"member":          m.name,
						"key":             key,
						"lastEventModRev": lastEventModRev,
						"gotEventModRev":  modRev,
						"regressionDelta": lastEventModRev - modRev,
						"respHeaderRev":   resp.Header.Revision,
						"watchStartRev":   startRev,
						"reconnects":      reconnects,
					})
				} else {
					lastEventModRev = modRev
					savescum.Always("etcd_watch_revision_monotonic", true, nil)
					if modRev%15 == 0 {
						savescum.Maximize("watch_rev_bucket_"+m.name, modRev/15)
					}
				}

				if !warmupSignaled && modRev >= warmupRev {
					warmupSignaled = true
					warmupReady <- struct{}{}
				}
			}
		}

		cancel()
		reconnects++
		if lastEventModRev > 0 {
			startRev = lastEventModRev + 1
		} else if committed := maxCommittedRev.Load(); committed > startRev {
			startRev = committed
		}
		time.Sleep(40 * time.Millisecond)
	}
}

func runFutureWatchWorker(ctx context.Context, members []*memberClient, maxCommittedRev *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		m := members[savescum.RandomU32()%uint32(len(members))]
		committed := maxCommittedRev.Load()
		offsets := []int64{5, 30, 120}
		offset := offsets[savescum.RandomU32()%uint32(len(offsets))]
		startRev := committed + offset

		wctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		wch := m.cli.Watch(wctx, "k/", clientv3.WithPrefix(), clientv3.WithRev(startRev))
		for resp := range wch {
			if resp.Err() != nil {
				break
			}
			for _, ev := range resp.Events {
				if ev.Kv.ModRevision < startRev {
					log.Printf("driver: BUG future_watch_old_event member=%s key=%s startRev=%d gotModRev=%d headerRev=%d",
						m.name, string(ev.Kv.Key), startRev, ev.Kv.ModRevision, resp.Header.Revision)
					savescum.Always("etcd_watch_start_rev_bound", false, map[string]any{
						"member":        m.name,
						"key":           string(ev.Kv.Key),
						"watchStartRev": startRev,
						"gotModRev":     ev.Kv.ModRevision,
						"delta":         startRev - ev.Kv.ModRevision,
						"respHeaderRev": resp.Header.Revision,
						"watchKind":     "future_rev",
					})
				} else {
					savescum.Always("etcd_watch_start_rev_bound", true, nil)
				}
			}
		}
		cancel()
		time.Sleep(30 * time.Millisecond)
	}
}

func runWriter(
	ctx context.Context,
	workerID int,
	profile SwarmProfile,
	quorumCli *clientv3.Client,
	maxCommittedRev *atomic.Int64,
) {
	seq := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		seq++
		key := fmt.Sprintf("k/%d", savescum.RandomU32()%uint32(profile.KeySpace))
		opCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
		var rev int64
		if profile.EnableDeletes && (savescum.RandomU32()%4) == 0 {
			resp, err := quorumCli.Delete(opCtx, key)
			if err == nil && resp != nil && resp.Header != nil {
				rev = resp.Header.Revision
			}
		} else {
			val := fmt.Sprintf("w%d-s%d-%08x", workerID, seq, savescum.RandomU32())
			resp, err := quorumCli.Put(opCtx, key, val)
			if err == nil && resp != nil && resp.Header != nil {
				rev = resp.Header.Revision
			}
		}
		cancel()

		if rev > 0 {
			for {
				prev := maxCommittedRev.Load()
				if rev <= prev || maxCommittedRev.CompareAndSwap(prev, rev) {
					break
				}
			}
			if rev%20 == 0 {
				savescum.Maximize("driver_committed_rev_bucket", rev/20)
			}
		}
		if profile.WritePaceMs > 0 {
			time.Sleep(time.Duration(profile.WritePaceMs) * time.Millisecond)
		}
	}
}

func runCompactor(ctx context.Context, quorumCli *clientv3.Client, maxCommittedRev *atomic.Int64) {
	var lastCompacted int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(150 * time.Millisecond):
		}
		cur := maxCommittedRev.Load()
		target := cur - 15
		if target > lastCompacted && target > 2 {
			cctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
			_, err := quorumCli.Compact(cctx, target)
			cancel()
			if err == nil {
				lastCompacted = target
				savescum.Reachable("driver_compacted")
			}
		}
	}
}

func runLinearizableReader(
	ctx context.Context,
	members []*memberClient,
	keySpace int,
	maxCommittedRev *atomic.Int64,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		m := members[savescum.RandomU32()%uint32(len(members))]
		key := fmt.Sprintf("k/%d", savescum.RandomU32()%uint32(keySpace))
		floorRev := maxCommittedRev.Load()
		if floorRev > 0 {
			rctx, cancel := context.WithTimeout(ctx, 2000*time.Millisecond)
			resp, err := m.cli.Get(rctx, key)
			cancel()
			if err == nil && resp != nil && resp.Header != nil {
				if resp.Header.Revision < floorRev {
					log.Printf("driver: BUG linearizable_read_monotonic member=%s key=%s minExpectedRev=%d gotHeaderRev=%d",
						m.name, key, floorRev, resp.Header.Revision)
					savescum.Always("etcd_linearizable_read_monotonic", false, map[string]any{
						"member":         m.name,
						"key":            key,
						"minExpectedRev": floorRev,
						"gotHeaderRev":   resp.Header.Revision,
						"staleDelta":     floorRev - resp.Header.Revision,
					})
				} else {
					savescum.Always("etcd_linearizable_read_monotonic", true, nil)
				}
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func runSwarmEpoch(
	profile SwarmProfile,
	duration time.Duration,
	members []*memberClient,
	quorumCli *clientv3.Client,
	maxCommittedRev *atomic.Int64,
) {
	log.Printf("driver: swarm epoch profile=%s", profile.Tag())
	savescum.Explore("swarm_profile", profile.Tag())

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	for w := 1; w <= profile.WriteWorkers; w++ {
		workerID := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWriter(ctx, workerID, profile, quorumCli, maxCommittedRev)
		}()
	}

	if profile.EnableLinReads {
		for r := 0; r < profile.ReadWorkers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				runLinearizableReader(ctx, members, profile.KeySpace, maxCommittedRev)
			}()
		}
	}

	if profile.EnableFutureWatches {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runFutureWatchWorker(ctx, members, maxCommittedRev)
		}()
	}

	if profile.EnableCompaction {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runCompactor(ctx, quorumCli, maxCommittedRev)
		}()
	}

	wg.Wait()
}

func main() {
	endpoints := parseMembers()
	members := make([]*memberClient, 0, len(endpoints))
	for i, ep := range endpoints {
		cli, err := dialEndpoints([]string{ep})
		if err != nil {
			log.Fatalf("driver: dial %s failed: %v", ep, err)
		}
		members = append(members, &memberClient{
			name:     memberName(ep, i),
			endpoint: ep,
			cli:      cli,
		})
	}
	quorumCli, err := dialEndpoints(endpoints)
	if err != nil {
		log.Fatalf("driver: dial quorum failed: %v", err)
	}

	initRev := waitForCluster(members, quorumCli)
	var maxCommittedRev atomic.Int64
	maxCommittedRev.Store(initRev)

	warmupReady := make(chan struct{}, len(members))
	warmupTargetRev := initRev + 1
	for _, m := range members {
		go runPersistentWatchVerifier(m, initRev, warmupTargetRev, warmupReady, &maxCommittedRev)
	}

	time.Sleep(80 * time.Millisecond)
	for attempt := 1; attempt <= 20; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := quorumCli.Put(ctx, "k/warmup", fmt.Sprintf("w%d", attempt))
		cancel()
		if err == nil && resp != nil && resp.Header != nil {
			maxCommittedRev.Store(resp.Header.Revision)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	deadline := time.After(5 * time.Second)
	for i := 0; i < len(members); i++ {
		select {
		case <-warmupReady:
		case <-deadline:
			log.Fatalf("driver: timed out waiting for all %d member watchers to sync", len(members))
		}
	}

	log.Printf("driver: all %d member watches synced; signaling setup_complete", len(members))
	savescum.SetupComplete()

	for {
		profile := sampleSwarmProfile()
		runSwarmEpoch(profile, 1500*time.Millisecond, members, quorumCli, &maxCommittedRev)
	}
}
