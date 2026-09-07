package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary
var locManila, _ = time.LoadLocation("Asia/Manila")

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Config từ env
var (
	chHost = getenv("CLICKHOUSE_HOST", "167.172.71.234")
	chPort = getenv("CLICKHOUSE_PORT", "9000")
	chUser = getenv("CLICKHOUSE_USER", "default")
	chPass = getenv("CLICKHOUSE_PASSWORD", "")
	chDB   = getenv("CLICKHOUSE_DB", "analytics")
)

// -----------------------------
// Request từ browser
// -----------------------------
type TrackRequest struct {
	SessionID  string `json:"session_id"`
	UserCookie string `json:"user_cookie"`
	Hostname   string `json:"hostname"`
	URL        string `json:"url"`
	Referer    string `json:"referer"`
	Domain     string `json:"domain"`
}

// -----------------------------
// Row ghi vào ClickHouse
// (thứ tự khớp với bảng analytics.user_activity)
// -----------------------------
type TrackEvent struct {
	ID         int64
	SessionID  string
	Hostname   string
	URL        string
	Referer    string
	Browser    string
	IsBot      int8
	UserCookie string
	IPUser     string
	Timestamp  int64
	DateNum    int32
	Domain     string
	CreatedAt  int64
}

// -----------------------------
// Bot detection
// -----------------------------
var botKeywords = []string{"bot", "spider", "crawl", "slurp", "bingpreview", "googlebot", "yahoo"}

func detectBot(ua string) int8 {
	lowUA := strings.ToLower(ua)
	for _, k := range botKeywords {
		if strings.Contains(lowUA, k) {
			return 1
		}
	}
	return 0
}

// -----------------------------
// Batch writer → ClickHouse native (port 9000)
// -----------------------------
type BatchWriter struct {
	mu      sync.Mutex
	buf     []TrackEvent
	maxSize int
	conn    clickhouse.Conn
}

func newBatchWriter() *BatchWriter {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chHost + ":" + chPort},
		Auth: clickhouse.Auth{
			Database: chDB,
			Username: chUser,
			Password: chPass,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		log.Fatalf("❌ ClickHouse connect error: %v", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		log.Fatalf("❌ ClickHouse ping failed: %v", err)
	}
	log.Printf("✅ ClickHouse connected: %s:%s db=%s", chHost, chPort, chDB)

	bw := &BatchWriter{
		buf:     make([]TrackEvent, 0, 5000),
		maxSize: 5000,
		conn:    conn,
	}
	go bw.periodicFlush()
	go bw.periodicOnlineAggregate()
	return bw
}

func (bw *BatchWriter) add(e TrackEvent) {
	bw.mu.Lock()
	bw.buf = append(bw.buf, e)
	full := len(bw.buf) >= bw.maxSize
	bw.mu.Unlock()
	if full {
		bw.flush()
	}
}

func (bw *BatchWriter) periodicFlush() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		bw.flush()
	}
}

func (bw *BatchWriter) flush() {
	bw.mu.Lock()
	if len(bw.buf) == 0 {
		bw.mu.Unlock()
		return
	}
	batch := bw.buf
	bw.buf = make([]TrackEvent, 0, bw.maxSize)
	bw.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	b, err := bw.conn.PrepareBatch(ctx, "INSERT INTO analytics.user_activity")
	if err != nil {
		log.Printf("❌ PrepareBatch error: %v (batch=%d)", err, len(batch))
		return
	}

	for i := range batch {
		e := &batch[i]
		if err := b.Append(
			e.ID, e.SessionID, e.Hostname, e.URL, e.Referer,
			e.Browser, e.IsBot, e.UserCookie, e.IPUser,
			e.Timestamp, e.DateNum, e.Domain, e.CreatedAt,
		); err != nil {
			log.Printf("⚠️  Append error: %v", err)
		}
	}

	if err := b.Send(); err != nil {
		log.Printf("❌ Batch send error: %v (batch=%d)", err, len(batch))
	} else {
		log.Printf("✅ Inserted %d events", len(batch))
	}
}

// -----------------------------
// Online users aggregator
// (thay thế aggregate_online() của kafka_consumer.py v1.0 đã tắt)
// -----------------------------
const onlineAggregateQuery = `
	INSERT INTO analytics.online_users_slots (timeslot, hostname, active_users)
	SELECT
		toStartOfInterval(toDateTime(timestamp, 'Asia/Manila'), INTERVAL 10 MINUTE) AS timeslot,
		hostname,
		uniqState(user_cookie) AS active_users
	FROM analytics.user_activity
	WHERE is_bot = 0
	  AND timestamp >= toUnixTimestamp(now()) - 2400
	GROUP BY timeslot, hostname
`

func (bw *BatchWriter) periodicOnlineAggregate() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		bw.aggregateOnline()
	}
}

func (bw *BatchWriter) aggregateOnline() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := bw.conn.Exec(ctx, onlineAggregateQuery); err != nil {
		log.Printf("⚠️  Online aggregate error: %v", err)
	} else {
		log.Printf("✅ Online slots updated")
	}
}

// -----------------------------
// Main
// -----------------------------
var bw *BatchWriter

func main() {
	bw = newBatchWriter()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(cors.New(cors.Config{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{"POST", "OPTIONS"},
		AllowHeaders: []string{"Origin", "Content-Type", "Accept"},
		MaxAge:       12 * time.Hour,
	}))

	r.POST("/track", trackHandler)
	r.OPTIONS("/track", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	log.Println("🚀 Tracking service running at :8001")
	r.Run(":8001")
}

// -----------------------------
// Track Handler
// -----------------------------
func trackHandler(c *gin.Context) {
	var req TrackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	// IP: ưu tiên CF header
	ipUser := c.GetHeader("cf-connecting-ip")
	if ipUser == "" {
		xff := c.GetHeader("x-forwarded-for")
		if idx := strings.Index(xff, ","); idx != -1 {
			ipUser = strings.TrimSpace(xff[:idx])
		} else {
			ipUser = strings.TrimSpace(xff)
		}
	}

	now := time.Now().In(locManila)
	ts := now.Unix()
	ua := c.Request.UserAgent()

	bw.add(TrackEvent{
		ID:         ts * 1000,
		SessionID:  req.SessionID,
		Hostname:   req.Hostname,
		URL:        req.URL,
		Referer:    req.Referer,
		Browser:    ua,
		IsBot:      detectBot(ua),
		UserCookie: req.UserCookie,
		IPUser:     ipUser,
		Timestamp:  ts,
		DateNum:    int32(now.Year()*10000 + int(now.Month())*100 + now.Day()),
		Domain:     req.Domain,
		CreatedAt:  ts,
	})

	c.String(http.StatusOK, `{"status":"ok"}`)
}
