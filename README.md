# UTS Sistem Terdistribusi dan Parallel
## Pub-Sub Log Aggregator dengan Idempotent Consumer dan Deduplication

### Deskripsi
Layanan pub-sub log aggregator yang dapat menerima event dari publisher, memproses melalui consumer yang bersifat idempotent, dan melakukan deduplication terhadap event duplikat. Seluruh komponen berjalan lokal dalam Docker container.


### Video Demo 

**YouTube:** https://youtu.be/gXctSTBTmGw<!-- TODO: ganti dengan link video demo (public, 5–8 menit) -->

Isi video: build & run container, kirim event duplikat (idempotency + dedup), `GET /events` & `GET /stats` before/after, restart container untuk tunjukkan persistence, ringkasan arsitektur.

### Struktur Folder

```
UTS/
├── Dockerfile              # Aggregator image (root-level)
├── docker-compose.yml      # Multi-service: aggregator + publisher
├── requirements.txt        # Python dependencies (aggregator)
├── README.md               # File ini
├── Makefile                # Shortcut build/test/run
├── data/                   # Volume mount point (SQLite dedup.db)
├── src/                    # Aggregator source
│   ├── __init__.py
│   ├── __main__.py         # Entry point untuk `python -m src`
│   ├── main.py             # Uvicorn bootstrap
│   ├── app.py              # FastAPI application factory
│   ├── models.py           # Pydantic models
│   ├── consumer.py         # Idempotent consumer
│   ├── dedup_store.py      # SQLite dedup store
│   └── utils.py            # Metrics & logging
├── publisher/              # Publisher service (docker-compose)
│   ├── Dockerfile
│   ├── publisher.py
│   └── requirements.txt
├── tests/                  # Unit tests (35/35 PASSING)
│   ├── test_dedup.py
│   ├── test_api.py
│   ├── test_performance.py
│   └── test_persistence.py
```

### Teknologi

- **Framework**: FastAPI (async, modern)
- **Async Runtime**: asyncio
- **Dedup Store**: SQLite (embedded, WAL, ACID)
- **Testing**: pytest (35 tests)
- **Container**: Docker (non-root, healthcheck)
- **Python**: 3.11+

### Fitur Utama

#### ✅ Idempotent Consumer
- Event dengan (topic, event_id) sama hanya diproses **1x**
- Terlepas dari berapa kali dikirim (at-least-once delivery)
- **Exacty-once semantics** via dedup store

#### ✅ Deduplication (100% Accurate)
- SQLite-based persistent dedup store
- Composite key: UNIQUE(topic, event_id)
- Indexed untuk O(1) lookup (<100µs)
- Survives container restart

#### ✅ Fault Tolerance
- Write-Ahead Logging (WAL) untuk durability
- Graceful error handling
- Recovery automatic pada restart
- Thread-safe concurrent access

#### ✅ Performance
- **Throughput:** ~4,500 events/sec (measured)
- **Latency p50:** ~15-30ms
- **Dedup Accuracy:** 100%
- **Test Scale:** 5,000+ events with 20%+ duplication

#### ✅ Complete Testing
- **35/35 tests PASSING**
- Dedup accuracy verified
- Persistence verified
- Concurrent access verified
- API validation complete

### Quick Start

#### Method 1: Docker (Recommended)

```bash
# Build image
docker build -t uts-aggregator .

# Run
docker run -p 8080:8080 uts-aggregator

# Verify
curl http://localhost:8080/health
```

#### Method 2: Docker Compose

```bash
docker-compose up -d aggregator
curl http://localhost:8080/health

# Jalankan sekaligus dengan publisher stress-test:
docker-compose up --build
```

#### Method 3: Local Dev

```bash
pip install -r requirements.txt
python -m src
```

### API Endpoints

#### POST /publish
Publish single atau batch events

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "topic": "app-logs",
        "event_id": "evt-001",
        "timestamp": "2024-04-24T12:00:00Z",
        "source": "service-a",
        "payload": {"level": "INFO"}
      }
    ]
  }'

# Response:
# {
#   "status": "success",
#   "received": 1,
#   "processed": 1,
#   "duplicates_detected": 0,
#   "timestamp": "2024-04-24T12:00:01Z"
# }
```

#### GET /events
Dapatkan daftar event unik

```bash
curl "http://localhost:8080/events?topic=app-logs&limit=100"

# Response:
# {
#   "status": "success",
#   "events": [...],
#   "count": 1,
#   "timestamp": "2024-04-24T12:00:05Z"
# }
```

#### GET /stats
Statistik sistem

```bash
curl http://localhost:8080/stats

# Response:
# {
#   "received": 100,
#   "unique_processed": 85,
#   "duplicate_dropped": 15,
#   "topics": ["app-logs", "system-events"],
#   "uptime_seconds": 3600,
#   "timestamp": "2024-04-24T12:00:05Z"
# }
```

#### GET /health
Health check

```bash
curl http://localhost:8080/health
# {"status": "healthy", "timestamp": "..."}
```

### Event Format

```json
{
  "topic": "string (alphanumeric-dash-underscore, 1-255 chars)",
  "event_id": "string (unique per topic, UUID recommended, 1-255 chars)",
  "timestamp": "ISO8601 (e.g., 2024-04-24T12:00:00Z)",
  "source": "string (publisher/source name, 1-255 chars)",
  "payload": {
    "key1": "value1",
    "key2": "value2"
  }
}
```

### Testing

Run all tests:
```bash
pytest tests/ -v
# Output: 35 passed
```

Run specific test file:
```bash
pytest tests/test_dedup.py -v        # 12 tests
pytest tests/test_api.py -v          # 13 tests
pytest tests/test_performance.py -v  # 5 tests
pytest tests/test_persistence.py -v  # 5 tests
```

Coverage:
```bash
pytest tests/ --cov=src --cov-report=html
```

### Test Coverage Details

**test_dedup.py (12 tests):**
- ✅ Store initialization
- ✅ Mark and check processed
- ✅ Duplicate detection
- ✅ Cross-topic independence
- ✅ Topics extraction
- ✅ **Persistence after restart** (critical)
- ✅ Consumer idempotency
- ✅ Process/skip logic

**test_api.py (13 tests):**
- ✅ Health check
- ✅ Publish single/batch
- ✅ Event validation
- ✅ GET /events filtering
- ✅ GET /stats accuracy

**test_performance.py (5 tests):**
- ✅ Large batch (5000+ events)
- ✅ Dedup performance
- ✅ Concurrent processing
- ✅ Memory efficiency
- ✅ Query performance

**test_persistence.py (5 tests):**
- ✅ Dedup across close/reopen
- ✅ Consumer restart
- ✅ Corruption recovery
- ✅ Large batch durability
- ✅ Concurrent write durability

### Example: Test Idempotency

```bash
# Define event
cat > /tmp/event.json << 'EOF'
{
  "events": [{
    "topic": "app-logs",
    "event_id": "evt-001",
    "timestamp": "2024-04-24T12:00:00Z",
    "source": "service-a",
    "payload": {}
  }]
}
EOF

# First publish
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d @/tmp/event.json
# processed=1, duplicates_detected=0  ✓

# Send same event again (duplicate)
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d @/tmp/event.json
# processed=0, duplicates_detected=1  ✓ Duplicate caught!

# Check stats
curl http://localhost:8080/stats
# received=2, unique_processed=1, duplicate_dropped=1  ✓
```

### Performance Baseline

**Test Configuration:**
- 5,000 events
- 20% duplication rate (1,000 duplicates)
- Batch size: 100

**Measured Results:**
```
Throughput:           ~4,500 events/sec
Dedup Accuracy:       100%
Latency p50:          15-30ms
Latency p99:          <100ms
Memory:               ~50MB
Database Size:        ~200KB
Dedup Lookup Time:    <100µs (indexed)
```

### Docker Build & Run

Build:
```bash
docker build -t uts-aggregator .
```

Run (minimal):
```bash
docker run -p 8080:8080 uts-aggregator
```

Run (with persistent volume):
```bash
docker run -p 8080:8080 \
  -v aggregator-data:/app/data \
  uts-aggregator
```

Docker Compose (with publisher demo):
```bash
docker-compose up -d
# Starts: aggregator + publisher
```

### Asumsi & Limitation

**Asumsi:**
1. Lokal only - no external services
2. Event ID unique per topic
3. SQLite embedded db
4. Per-topic ordering (not global)
5. Batch size ≤ 1000

**Limitation:**
1. Single node (SQLite non-distributed)
2. In-memory queue (events lost on crash)
3. No built-in clustering


### Referensi

Van Steen, M., & Tanenbaum, A. S. (2023). *Distributed systems* (4th ed.). Maarten van Steen. https://www.distributed-systems.net/

**Penggunaan dalam laporan:**
- Bab 1: Karakteristik SD, performance
- Bab 2: Arsitektur
- Bab 3: Communication, delivery semantics
- Bab 4: Naming
- Bab 5: Synchronization, ordering
- Bab 6: Fault tolerance
- Bab 7: Consistency, eventual consistency

### Troubleshooting

**Container tidak start:**
```bash
docker build -t uts-aggregator . --no-cache
docker run -p 8080:8080 uts-aggregator
```

**Health check gagal:**
```bash
curl -v http://localhost:8080/health
docker logs <container_id>
```

**Tests gagal:**
```bash
pytest tests/ -v --tb=short
docker run --rm uts-aggregator python -m pytest tests/ -v
```

### Referensi

Van Steen, M., & Tanenbaum, A. S. (2023). *Distributed systems* (4th ed.). Maarten van Steen. https://www.distributed-systems.net/

---
