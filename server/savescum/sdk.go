package savescum

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultChannelPath = "/dev/savescum"
	defaultUrandomPath = "/dev/urandom"
	maxTokenLen        = 120
)

var (
	mu          sync.Mutex
	inited      bool
	channelFile *os.File
	bridgeURL   string
	httpClient  = &http.Client{Timeout: 500 * time.Millisecond}

	seenReachable = map[string]struct{}{}
	seenExplore   = map[string]struct{}{}
	seenAlwaysOk  = map[string]struct{}{}
	localMax      = map[string]int64{}
)

func initLocked() {
	if inited {
		return
	}
	inited = true

	chPath := strings.TrimSpace(os.Getenv("SAVESCUM_CHANNEL_PATH"))
	if chPath == "" {
		chPath = defaultChannelPath
	}
	if f, err := os.OpenFile(chPath, os.O_WRONLY, 0); err == nil {
		channelFile = f
	}

	bURL := strings.TrimRight(strings.TrimSpace(os.Getenv("SAVESCUM_BRIDGE_URL")), "/")
	if bURL != "" {
		bridgeURL = bURL
	}
}

func sanitizeToken(raw string) string {
	var b strings.Builder
	prevUnderscore := true
	for _, r := range raw {
		allowed := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '_' || r == '.' || r == ':' || r == '/' || r == '-'
		if allowed {
			b.WriteRune(r)
			prevUnderscore = false
		} else if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
		if b.Len() >= maxTokenLen {
			break
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "empty"
	}
	return out
}

func escapeJSONWhitespace(raw string) string {
	var b strings.Builder
	for _, r := range raw {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' || r < 0x20 || (r >= 0x7f && r <= 0x9f) {
			fmt.Fprintf(&b, "\\u%04x", r)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func postBridge(line string) bool {
	if bridgeURL == "" {
		return false
	}
	resp, err := httpClient.Post(bridgeURL+"/sdk", "text/plain", bytes.NewBufferString(line+"\n"))
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func writeChannelLocked(line string) {
	if channelFile != nil {
		_, _ = channelFile.WriteString(line + "\n")
	}
}

func emit(line string, preferBridge bool) {
	mu.Lock()
	initLocked()
	bURL := bridgeURL
	hasChan := channelFile != nil
	if !preferBridge && hasChan {
		writeChannelLocked(line)
		mu.Unlock()
		return
	}
	mu.Unlock()

	if bURL != "" && postBridge(line) {
		return
	}
	mu.Lock()
	writeChannelLocked(line)
	mu.Unlock()
}

func SetupComplete() {
	emit("sdk setup_complete", true)
}

func Reachable(id string) {
	clean := sanitizeToken(id)
	mu.Lock()
	initLocked()
	if _, ok := seenReachable[clean]; ok {
		mu.Unlock()
		return
	}
	seenReachable[clean] = struct{}{}
	mu.Unlock()
	emit("sdk reachable id="+clean, false)
}

func Explore(id, key string) {
	cleanID := sanitizeToken(id)
	cleanKey := sanitizeToken(key)
	composite := cleanID + "\x00" + cleanKey
	mu.Lock()
	initLocked()
	if _, ok := seenExplore[composite]; ok {
		mu.Unlock()
		return
	}
	seenExplore[composite] = struct{}{}
	mu.Unlock()
	emit("sdk explore id="+cleanID+" key="+cleanKey, false)
}

func Maximize(id string, value int64) {
	cleanID := sanitizeToken(id)
	mu.Lock()
	initLocked()
	if prev, ok := localMax[cleanID]; ok && value <= prev {
		mu.Unlock()
		return
	}
	localMax[cleanID] = value
	mu.Unlock()
	emit(fmt.Sprintf("sdk maximize id=%s value=%d", cleanID, value), false)
}

func Always(id string, ok bool, detail map[string]any) {
	cleanID := sanitizeToken(id)
	if ok {
		mu.Lock()
		initLocked()
		if _, seen := seenAlwaysOk[cleanID]; seen {
			mu.Unlock()
			return
		}
		seenAlwaysOk[cleanID] = struct{}{}
		mu.Unlock()
		emit("sdk always id="+cleanID+" ok=true", false)
		return
	}

	line := "sdk always id=" + cleanID + " ok=false"
	if len(detail) > 0 {
		if encoded, err := json.Marshal(detail); err == nil {
			line += " detail=" + escapeJSONWhitespace(string(encoded))
		}
	}
	emit(line, true)
}

func RandomU32() uint32 {
	f, err := os.Open(defaultUrandomPath)
	if err == nil {
		defer f.Close()
		var buf [4]byte
		if _, err := f.Read(buf[:]); err == nil {
			return binary.LittleEndian.Uint32(buf[:])
		}
	}
	return uint32(time.Now().UnixNano())
}
