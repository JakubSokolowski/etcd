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

func runWatchVerifier(
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
				log.Printf("driver: watch %s closed with err=%v (lastEventModRev=%d)", m.name, err, lastEventModRev)
				break
			}
			if resp.Created {
				log.Printf("driver: watch %s created startRev=%d headerRev=%d", m.name, startRev, resp.Header.Revision)
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
						"issue":         "etcd-io/etcd#20221",
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
						"issue":           "etcd-io/etcd#15271",
					})
				} else {
					lastEventModRev = modRev
					savescum.Always("etcd_watch_revision_monotonic", true, nil)
					savescum.Maximize("watch_rev_"+m.name, modRev)
				}

				if !warmupSignaled && modRev >= warmupRev {
					warmupSignaled = true
					warmupReady <- struct{}{}
				}
			}
		}

		cancel()
		reconnects++
		savescum.Explore("watch_reconnect", fmt.Sprintf("%s_r%d", m.name, minInt(reconnects, 6)))
		if lastEventModRev > 0 {
			startRev = lastEventModRev + 1
		} else if committed := maxCommittedRev.Load(); committed > startRev {
			startRev = committed
		}
		time.Sleep(40 * time.Millisecond)
	}
}

func runWriter(workerID int, quorumCli *clientv3.Client, maxCommittedRev *atomic.Int64) {
	seq := 0
	for {
		seq++
		key := fmt.Sprintf("k/%d", savescum.RandomU32()%8)
		val := fmt.Sprintf("w%d-s%d-%08x", workerID, seq, savescum.RandomU32())
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		resp, err := quorumCli.Put(ctx, key, val)
		cancel()
		if err == nil && resp != nil && resp.Header != nil {
			rev := resp.Header.Revision
			for {
				prev := maxCommittedRev.Load()
				if rev <= prev || maxCommittedRev.CompareAndSwap(prev, rev) {
					break
				}
			}
			if rev%5 == 0 {
				savescum.Maximize("driver_committed_rev_bucket", rev/5)
			}
		}
		time.Sleep(3 * time.Millisecond)
	}
}

func runLinearizableReader(members []*memberClient, maxCommittedRev *atomic.Int64) {
	idx := 0
	for {
		m := members[idx%len(members)]
		idx++
		floorRev := maxCommittedRev.Load()
		if floorRev > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			resp, err := m.cli.Get(ctx, "k/0")
			cancel()
			if err == nil && resp != nil && resp.Header != nil {
				if resp.Header.Revision < floorRev {
					log.Printf("driver: BUG linearizable_read_monotonic member=%s minExpectedRev=%d gotHeaderRev=%d",
						m.name, floorRev, resp.Header.Revision)
					savescum.Always("etcd_linearizable_read_monotonic", false, map[string]any{
						"member":         m.name,
						"minExpectedRev": floorRev,
						"gotHeaderRev":   resp.Header.Revision,
						"staleDelta":     floorRev - resp.Header.Revision,
						"issue":          "etcd-io/etcd#20418",
					})
				} else {
					savescum.Always("etcd_linearizable_read_monotonic", true, nil)
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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
		go runWatchVerifier(m, initRev, warmupTargetRev, warmupReady, &maxCommittedRev)
	}

	// Give watchers a moment to register on all 3 members, then emit warmup write.
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

	// Wait for all 3 per-member watchers to be synced and receiving live events.
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

	var wg sync.WaitGroup
	wg.Add(5)
	for w := 1; w <= 4; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			runWriter(workerID, quorumCli, &maxCommittedRev)
		}()
	}
	go func() {
		defer wg.Done()
		runLinearizableReader(members, &maxCommittedRev)
	}()
	wg.Wait()
}
