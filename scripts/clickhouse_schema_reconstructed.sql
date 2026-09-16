-- ==========================================================================
-- Reconstructed schema for `analytics` database on the ORIGINAL ClickHouse
-- server (167.172.71.234, destroyed on DigitalOcean, no backup existed).
--
-- IMPORTANT: this is NOT a copy of the original DDL (SSH access to that box
-- never existed in any session). It is reverse-engineered purely from the
-- INSERT/SELECT statements and system.tables engine types observed while
-- working against it. Column NAMES and roughly-correct TYPES are high
-- confidence; ORDER BY / PARTITION BY keys and the exact choice between
-- AggregateFunction vs SimpleAggregateFunction vs plain numeric columns on
-- the aggregate tables are best-effort reconstructions and may not exactly
-- match the original engine behavior. Review before applying.
-- ==========================================================================

CREATE DATABASE IF NOT EXISTS analytics;

-- --------------------------------------------------------------------------
-- user_activity — raw tracking events (main table, high volume)
-- Written by: tracking-go (direct on dashboard) and kafka_consumer.py
--   (from Redpanda, on behalf of BE's tracking-go producer)
-- Confirmed column order from Go INSERT and Python batch INSERT:
--   id, session_id, hostname, url, referer, browser, is_bot, user_cookie,
--   ip_user, timestamp, date_num, domain, created_at
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.user_activity
(
    id          Int64,
    session_id  String,
    hostname    String,
    url         String,
    referer     String,
    browser     String,
    is_bot      UInt8,
    user_cookie String,
    ip_user     String,
    timestamp   UInt32,
    date_num    Int32,
    domain      String,
    created_at  UInt32
)
ENGINE = MergeTree
PARTITION BY date_num
ORDER BY (hostname, timestamp)
SETTINGS index_granularity = 8192;

-- --------------------------------------------------------------------------
-- online_users_slots — 10-minute concurrent-viewer buckets
-- Written by: periodicOnlineAggregate() in tracking-go (every 30s) via
--   INSERT INTO ... (timeslot, hostname, active_users)
--   SELECT toStartOfInterval(...), hostname, uniqState(user_cookie)
-- Read by: FastAPI /online via uniqMerge(active_users) ... FINAL
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.online_users_slots
(
    timeslot     DateTime,
    hostname     String,
    active_users AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(timeslot)
ORDER BY (timeslot, hostname);

-- --------------------------------------------------------------------------
-- daily_hostname_summary — per-day per-hostname rollup
-- Written by: kafka_consumer.py aggregate_daily(), every 120s, via
--   INSERT INTO ... (date_num, hostname, total_user_activate,
--     total_view_page, total_bot_sessions, sum_stay_duration,
--     total_sessions, unique_bot_sessions)
--   SELECT date_num, hostname,
--     uniqStateIf(user_cookie, is_bot=0),      -- total_user_activate
--     uniqState(concat(session_id,url)),       -- total_view_page
--     sum(is_bot),                             -- total_bot_sessions
--     sum(if(is_bot=0,10,0)),                  -- sum_stay_duration
--     uniqState(session_id),                   -- total_sessions
--     uniqStateIf(session_id, is_bot=1)        -- unique_bot_sessions
-- Read by: /summary via uniqMerge(total_user_activate/total_view_page/
--   unique_bot_sessions/total_sessions), plain sum semantics assumed for
--   total_bot_sessions/sum_stay_duration (hence SimpleAggregateFunction
--   below so merges sum them instead of picking an arbitrary row's value).
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.daily_hostname_summary
(
    date_num            Int32,
    hostname            String,
    total_user_activate AggregateFunction(uniq, String),
    total_view_page     AggregateFunction(uniq, String),
    total_bot_sessions  SimpleAggregateFunction(sum, UInt64),
    sum_stay_duration   SimpleAggregateFunction(sum, UInt64),
    total_sessions      AggregateFunction(uniq, String),
    unique_bot_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(toDate(toString(date_num), 'YYYYMMDD'))
ORDER BY (date_num, hostname);

-- --------------------------------------------------------------------------
-- user_first_seen — first-seen date per (user_cookie, hostname), used to
-- compute "new users" (joined against in /summary's new_users_map CTE)
-- Exact original columns never directly observed in an INSERT statement
-- this session — reconstructed from how /summary reads it:
--   SELECT user_cookie, hostname WHERE first_date = '{date_str}'
-- ReplacingMergeTree so a cookie's first-seen date, once written, wins.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.user_first_seen
(
    user_cookie String,
    hostname    String,
    first_date  Date
)
ENGINE = ReplacingMergeTree
ORDER BY (user_cookie, hostname);

-- --------------------------------------------------------------------------
-- Secondary/low-traffic tables — never seen written to or read from this
-- session; included for completeness so nothing 500s on a missing table,
-- but definitions are guesses and should be revisited if actually used.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.temp_user_activity
(
    id               Int64,
    ip_user          String,
    browser          String,
    referer          String,
    stay_duration    Int64,
    hostname         String,
    url              String,
    user_cookie      String,
    timestamp        DateTime,
    is_bot           UInt8,
    date_num         Int32,
    timestamp_int    Int64,
    domain_id        Int64,
    label_id         Int64,
    is_updated_label UInt8,
    domain           String,
    created_at       DateTime
)
ENGINE = MergeTree
PARTITION BY date_num
ORDER BY (hostname, timestamp);

CREATE TABLE IF NOT EXISTS analytics.forecast_cache
(
    hostname   String,
    date_num   Int32,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (hostname, date_num);

CREATE TABLE IF NOT EXISTS analytics.traffic_anomalies
(
    hostname    String,
    date_num    Int32,
    detected_at DateTime DEFAULT now(),
    description String
)
ENGINE = MergeTree
ORDER BY (hostname, date_num);

-- --------------------------------------------------------------------------
-- Deliberately NOT recreated: user_activity_kafka (Kafka Engine, both in
-- `analytics` and `default` schemas) and its mv_* materialized views.
-- Confirmed dead: 0 messages ever consumed in 28+ days (broken SSL cert
-- against the DO Kafka broker that no longer exists anyway). No reason to
-- rebuild a native ClickHouse->Kafka path now that Redpanda + the Python
-- consumer already do this job.
-- --------------------------------------------------------------------------
