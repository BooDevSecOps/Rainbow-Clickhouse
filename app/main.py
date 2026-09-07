import math
import os
import asyncio
import time
import ipaddress
import json
import csv
import io
import redis
import ssl
from datetime import datetime, timedelta
import requests
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
import pytz
from clickhouse_driver import Client
from user_agents import parse
from confluent_kafka import Producer
from pydantic import BaseModel
from typing import List
from google import genai
from .config import (
    GOOGLE_API_KEY, REDIS_URI, AGENT_BASE_URL, AGENT_API_KEY, AGENT_MODEL,
    KAFKA_BROKER, KAFKA_USERNAME, KAFKA_PASSWORD,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB,
    CLOUDFLARE_IPV4_LIST_URL, CLOUDFLARE_IPV6_LIST_URL, CLOUDFLARE_LIST_TOKEN
)
from .agent_integration import AgentIntegration

tz = pytz.timezone("Asia/Manila")
app = FastAPI(title="User Activity Analytics")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True, # Thêm dòng này để hỗ trợ Cookies/Auth
    allow_methods=["*"],
    allow_headers=["*"],
)
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'security.protocol': "SASL_SSL",
    'sasl.mechanisms': "PLAIN",
    'sasl.username': KAFKA_USERNAME,
    'sasl.password': KAFKA_PASSWORD,
    
    # THAY THẾ CHO ssl_context:
    'ssl.endpoint.identification.algorithm': 'none', # Tương đương check_hostname = False
    'enable.ssl.certificate.verification': False,    # Tương đương verify_mode = CERT_NONE
    
    # Giữ nguyên các config tối ưu tải lớn
    'linger.ms': 100,
    'batch.num.messages': 5000,
    'queue.buffering.max.messages': 1000000,
    'compression.type': 'lz4',
    'acks': 1
}

# --- Redis Client ---
if REDIS_URI:
    redis_client = redis.from_url(REDIS_URI, decode_responses=True)
else:
    print("⚠️ Warning: REDIS_URI not found in .env. Redis caching is disabled.")
    redis_client = None

# --- Agent Client (New Integration) ---
agent_client = AgentIntegration(AGENT_BASE_URL, AGENT_API_KEY, AGENT_MODEL)


def get_client_ip(request: Request):
    headers = request.headers
    # Ưu tiên Cloudflare
    if "cf-connecting-ip" in headers:
        return headers["cf-connecting-ip"]
    # Fallback
    if "x-forwarded-for" in headers:
        return headers["x-forwarded-for"].split(",")[0].strip()

    return request.client.host
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        pass

@app.post("/track")
async def track(request: Request):
    # 1. Lấy dữ liệu nhanh
    body = await request.json()
    timestamp = int(time.time())
    tz = pytz.timezone("Asia/Manila")
    
    # 2. Lấy IP tối ưu
    headers = request.headers
    ip_user = headers.get("cf-connecting-ip") or \
              headers.get("x-forwarded-for", "").split(",")[0] or \
              request.client.host

    # 3. Chuẩn bị event
    event = {
        "id": timestamp * 1000,
        "session_id": body.get("session_id"),
        "user_cookie": body.get("user_cookie"),
        "hostname": body.get("hostname", ""),
        "url": body.get("url", ""),
        "referer": body.get("referer", ""),
        "browser": headers.get("user-agent", ""),
        "is_bot": 0,
        "ip_user": ip_user,
        "timestamp": timestamp,
        "date_num": int(datetime.now(tz).strftime("%Y%m%d")),
        "domain": body.get("domain", ""),
        "created_at": timestamp
    }

    # 4. Gửi vào Kafka (Cực nhanh - Non-blocking)
    try:
        producer.produce(
            "user-activity", 
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        # Kích hoạt gửi ngầm, không chờ đợi
        producer.poll(0)
    except BufferError:
        # Nếu hàng đợi quá đầy, trả lỗi 503 ngay để Nginx giải phóng kết nối
        return {"status": "error", "message": "Queue full"}, 503

    return {
        "status": "ok", 
        "session_id": event["session_id"]
    }


# ---------------------------
# 1. ClickHouse client factory
# ---------------------------
def get_clickhouse_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=9000,
        user=CLICKHOUSE_USER,
        password="",
        database=CLICKHOUSE_DB
    )

@app.get("/summary")
def get_summary(page: int = 1, limit: int = 20, date: str = None):
    # 1. Xử lý thời gian
    manila_tz = ZoneInfo("Asia/Manila")
    if not date or date == "undefined":
        now = datetime.now(manila_tz)
        date_str = now.strftime("%Y-%m-%d")
        date_int = int(now.strftime("%Y%m%d"))
    else:
        date_str = date
        date_int = int(date.replace("-", ""))

    offset = (page - 1) * limit
    client = get_clickhouse_client()

    try:
        # QUERY 1: Lấy Grand Total 
        # Tối ưu: Gộp New Users tổng bằng Subquery (Bỏ FINAL để tránh treo server)
        total_query = f"""
            SELECT 
                uniqMerge(total_user_activate) as g_users,
                uniqMerge(total_view_page) as g_views,
                uniqMerge(unique_bot_sessions) as g_bots,
                (
                    SELECT count() FROM (
                        SELECT user_cookie FROM analytics.user_first_seen 
                        WHERE first_date = '{date_str}'
                        GROUP BY user_cookie
                    )
                ) as g_new_users
            FROM analytics.daily_hostname_summary
            WHERE date_num = {date_int}
        """
        
        g_res = client.execute(total_query)
        
        # Xử lý kết quả Grand Total
        if g_res:
            g_users, g_views, g_bots, g_new_users = g_res[0]
        else:
            g_users, g_views, g_bots, g_new_users = 0, 0, 0, 0

        grand_total = {
            "total_users": g_users,
            "total_views": g_views,
            "total_bots": g_bots,
            "total_new_users": g_new_users
        }

        # QUERY 2: Lấy danh sách Hostname
        # Tối ưu: Sử dụng WITH clause để tính New Users theo hostname mà không dùng JOIN FINAL
        query = f"""
            WITH 
                new_users_map AS (
                    SELECT hostname, count() as cnt
                    FROM (
                        SELECT hostname, user_cookie FROM analytics.user_first_seen
                        WHERE first_date = '{date_str}'
                        GROUP BY hostname, user_cookie
                    )
                    GROUP BY hostname
                )
            SELECT
                '{date_str}' AS date,
                base.hostname,
                base.total_user_activate,
                base.total_view_page,
                coalesce(nu.cnt, 0) AS new_user,
                round(base.sum_stay_duration / NULLIF(base.total_sessions_count, 0)) AS avg_stay_duration,
                base.total_bot_sessions,
                COUNT() OVER() AS total_rows
            FROM (
                SELECT
                    hostname,
                    uniqMerge(total_user_activate) AS total_user_activate,
                    uniqMerge(total_view_page) AS total_view_page,
                    uniqMerge(unique_bot_sessions) AS total_bot_sessions,
                    sum(sum_stay_duration) AS sum_stay_duration,
                    uniqMerge(total_sessions) AS total_sessions_count
                FROM analytics.daily_hostname_summary
                WHERE date_num = {date_int}
                GROUP BY hostname
            ) base
            LEFT JOIN new_users_map nu ON base.hostname = nu.hostname
            ORDER BY total_user_activate DESC
            LIMIT {limit} OFFSET {offset}
        """
        
        result = client.execute(query)

    except Exception as e:
        return {"error": str(e)}
    finally:
        client.disconnect()

    # 2. Parse dữ liệu trả về
    data = []
    total_count = 0
    if result:
        total_count = result[0][7]
        data = [{
            "date": r[0], 
            "hostname": r[1], 
            "total_user_activate": r[2],
            "total_view_page": r[3], 
            "new_user": r[4],
            "avg_stay_duration": int(r[5]) if r[5] else 0, 
            "total_bots": r[6]
        } for r in result]

    return {
        "page": page,
        "limit": limit,
        "total": total_count,
        "total_pages": math.ceil(total_count / limit) if limit > 0 else 0,
        "grand_total": grand_total,
        "data": data
    }

# ---------------------------
# 3. Query user detail
# ---------------------------
def query_user_detail(page: int, limit: int):
    tz = pytz.timezone("Asia/Manila")
    date_num_today = int(datetime.now(tz).strftime("%Y%m%d"))
    offset = (page - 1) * limit

    # Thêm COUNT() OVER() để lấy tổng số dòng cho phân trang
    query = f"""
    SELECT
        session_id,
        user_cookie,
        argMax(hostname, timestamp) AS hostname,  -- Lấy hostname mới nhất của session
        argMax(url, timestamp) AS url,            -- Lấy URL cuối cùng họ xem
        any(referer) AS referer,
        any(browser) AS browser,
        any(is_bot) AS is_bot,
        any(ip_user) AS ip_user,
        any(domain) AS domain,
        min(timestamp) AS first_ts,
        max(timestamp) AS last_ts,
        (max(timestamp) - min(timestamp)) AS stay_duration,
        date_num,
        count() OVER() AS total_count             -- Tổng số session để tính total_pages
    FROM analytics.user_activity
    WHERE date_num = {date_num_today}
    GROUP BY
        session_id,
        user_cookie,
        date_num
    ORDER BY last_ts DESC
    LIMIT {limit} OFFSET {offset}
    """

    ch = get_clickhouse_client()
    try:
        rows = ch.execute(query)
    finally:
        ch.disconnect()

    if not rows:
        return {"total": 0, "list": []}

    total_count = rows[0][13]
    data_list = [
        {
            "session_id": r[0],
            "user_cookie": r[1],
            "hostname": r[2],
            "url": r[3],
            "referer": r[4],
            "browser": r[5],
            "is_bot": r[6],
            "ip_user": r[7],
            "domain": r[8],
            "first_ts": r[9],
            "last_ts": r[10],
            "stay_duration": int(r[11]),
            "date_num": r[12]
        } for r in rows
    ]
    
    return {"total": total_count, "list": data_list}

@app.get("/detail")
def get_user_detail(
    page: int = Query(1, ge=1), 
    limit: int = Query(50, ge=1, le=1000),
    ip: str = Query(None)  # Thêm tham số search IP
):
    tz = pytz.timezone("Asia/Manila")
    date_num_today = int(datetime.now(tz).strftime("%Y%m%d"))
    offset = (page - 1) * limit

    # Xử lý điều kiện lọc IP
    ip_filter = ""
    if ip and ip.strip():
        # Sử dụng LIKE để có thể search part của IP (ví dụ: "192.168")
        ip_filter = f"AND ip_user LIKE '%{ip.strip()}%'"

    query = f"""
    SELECT
        session_id, user_cookie, argMax(hostname, timestamp), argMax(url, timestamp),
        any(referer), any(is_bot), any(ip_user), 
        min(timestamp), max(timestamp), (max(timestamp) - min(timestamp)), 
        count() OVER() AS total_count
    FROM analytics.user_activity
    WHERE date_num = {date_num_today} {ip_filter}
    GROUP BY session_id, user_cookie, date_num
    ORDER BY max(timestamp) DESC
    LIMIT {limit} OFFSET {offset}
    """
    
    ch = get_clickhouse_client()
    try:
        rows = ch.execute(query)
    except Exception as e:
        return {"error": str(e), "data": [], "total_pages": 0}
    finally:
        ch.disconnect()

    total_records = rows[0][10] if rows else 0
    data = [{
        "session_id": r[0], "hostname": r[2], "url": r[3], "is_bot": r[5],
        "ip_user": r[6], "first_ts": r[7], "last_ts": r[8], "stay_duration": int(r[9])
    } for r in rows]

    return {
        "page": page, "limit": limit, "total_records": total_records,
        "total_pages": math.ceil(total_records / limit) if limit > 0 else 0,
        "data": data
    }


@app.get("/dashboard/summary", response_class=HTMLResponse)
def summary_ui(request: Request):
    return templates.TemplateResponse("summary.html", {"request": request, "active_page": "summary"})


@app.get("/dashboard/details", response_class=HTMLResponse)
def detail_ui(request: Request):
    return templates.TemplateResponse("details.html", {"request": request, "active_page": "details"})

def query_users_online(page: int, limit: int):
    offset = (page - 1) * limit

    query = f"""
    SELECT
        hostname,
        uniqMerge(active_users) AS online_count,
        COUNT() OVER() AS total_rows
    FROM analytics.online_users_slots FINAL
    WHERE timeslot >= subtractMinutes(now(), 30)
    GROUP BY hostname
    ORDER BY online_count DESC
    LIMIT {int(limit)} OFFSET {int(offset)}
    """

    ch = get_clickhouse_client()
    try:
        rows = ch.execute(query)
    finally:
        ch.disconnect()

    if not rows:
        return [], 0

    total_count = rows[0][2]
    data = [
        {"hostname": r[0], "active_users": r[1]} for r in rows
    ]

    return data, total_count

@app.get("/online")
async def users_online_api(page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=100)):
    loop = asyncio.get_event_loop()
    data, total = await loop.run_in_executor(None, query_users_online, page, limit)
    return JSONResponse({
        "page": page,
        "limit": limit,
        "total": total,
        "total_pages": math.ceil(total / limit),
        "data": data
    })

@app.get("/", response_class=HTMLResponse)
def online_dashboard(request: Request):
    return templates.TemplateResponse("online.html", {"request": request, "active_page": "online"})

# ---------------------------
# 5. Daily Report Generation
# ---------------------------
class AnalyzeRequest(BaseModel):
    date: str = None
    hostname: str = None

def fetch_daily_report_context(date_input: str = None, hostname_input: str = None):
    # 1. Prepare Date
    tz = pytz.timezone("Asia/Manila")
    now_manila = datetime.now(tz)
    
    if date_input:
        target_date = date_input
    else:
        target_date = now_manila.strftime("%Y-%m-%d")

    date_int = int(target_date.replace("-", ""))
    today_int = int(now_manila.strftime("%Y%m%d"))
    
    # --- REDIS CACHE CHECK ---
    is_past = date_int < today_int
    is_today_check = date_int == today_int
    should_cache = is_past or is_today_check
    cache_key = f"report_ctx:{date_int}:{hostname_input.strip() if hostname_input else 'all'}"
    
    if redis_client and should_cache:
        try:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
        except Exception as e:
            print(f"[Redis Error] Get: {e}")

    dt_obj = datetime.strptime(target_date, "%Y-%m-%d")
    yesterday_int = int((dt_obj - timedelta(days=1)).strftime("%Y%m%d"))
    last_week_int = int((dt_obj - timedelta(days=7)).strftime("%Y%m%d"))

    # Prepare Hostname Filter
    hostname_filter = ""
    if hostname_input and hostname_input.strip():
        hostname_filter = f"AND hostname = '{hostname_input.strip()}'"

    # 2. Fetch Data Context from ClickHouse
    ch = get_clickhouse_client()
    try:
        # A. Grand Summary (Users, Views, Bots)
        sql_summary = f"""
            SELECT date_num, uniqMerge(total_user_activate), uniqMerge(total_view_page), uniqMerge(unique_bot_sessions)
            FROM analytics.daily_hostname_summary WHERE date_num IN ({date_int}, {yesterday_int}, {last_week_int}) {hostname_filter}
            GROUP BY date_num
        """
        sum_res = ch.execute(sql_summary)
        summary_map = {
            date_int: {"users": 0, "views": 0, "bots": 0},
            yesterday_int: {"users": 0, "views": 0, "bots": 0},
            last_week_int: {"users": 0, "views": 0, "bots": 0}
        }
        for r in sum_res:
            summary_map[r[0]] = {"users": r[1], "views": r[2], "bots": r[3]}
        summary_data = summary_map[date_int]
        yesterday_data = summary_map[yesterday_int]
        last_week_data = summary_map[last_week_int]

        # --- LOGIC MỚI: Nếu là hôm nay, so sánh với "Cùng giờ hôm qua" ---
        is_today = (date_int == int(now_manila.strftime("%Y%m%d")))
        comparison_suffix = ""
        
        if is_today:
            current_hour = now_manila.hour
            comparison_suffix = " (same time)"
            
            # Query dữ liệu từng phần (Partial Data) của hôm qua và tuần trước
            sql_partial = f"""
                SELECT date_num, uniq(user_cookie), count(), uniqIf(session_id, is_bot=1)
                FROM analytics.user_activity 
                WHERE date_num IN ({yesterday_int}, {last_week_int}) 
                  AND toHour(toDateTime(timestamp, 'Asia/Manila')) <= {current_hour}
                  {hostname_filter}
                GROUP BY date_num
            """
            partial_res = ch.execute(sql_partial)
            for r in partial_res:
                if r[0] == yesterday_int:
                    yesterday_data = {"users": r[1], "views": r[2], "bots": r[3]}
                elif r[0] == last_week_int:
                    last_week_data = {"users": r[1], "views": r[2], "bots": r[3]}

        # B. Top 5 Hostnames
        sql_top = f"""
            SELECT hostname, uniqMerge(total_user_activate) as u, uniqMerge(total_view_page) as v, uniqMerge(unique_bot_sessions) as b
            FROM analytics.daily_hostname_summary WHERE date_num = {date_int} {hostname_filter}
            GROUP BY hostname ORDER BY u DESC LIMIT 5
        """
        top_res = ch.execute(sql_top)
        top_hosts = [{"host": r[0], "users": r[1], "views": r[2], "bots": r[3]} for r in top_res]

        # C. Hourly Trend (Peak hours analysis)
        sql_hourly = f"""
            SELECT toHour(toDateTime(timestamp, 'Asia/Manila')) as h, uniq(user_cookie)
            FROM analytics.user_activity WHERE date_num = {date_int} {hostname_filter} GROUP BY h ORDER BY h
        """
        hour_res = ch.execute(sql_hourly)
        hourly_trend = {r[0]: r[1] for r in hour_res}

        # D. Top Sources (Referrers) - New Insight
        sql_ref = f"""
            SELECT if(referer='', 'Direct / Bookmark', domain(referer)) as src, uniq(session_id) as c
            FROM analytics.user_activity WHERE date_num = {date_int} {hostname_filter}
            GROUP BY src ORDER BY c DESC LIMIT 3
        """
        ref_res = ch.execute(sql_ref)
        top_refs = [{"source": r[0], "count": r[1]} for r in ref_res]

        # E. Top Pages (Most Viewed URLs) - New Insight
        sql_url = f"""
            SELECT url, count() as c
            FROM analytics.user_activity WHERE date_num = {date_int} {hostname_filter}
            GROUP BY url ORDER BY c DESC LIMIT 3
        """
        url_res = ch.execute(sql_url)
        top_urls = [{"url": r[0], "count": r[1]} for r in url_res]

        # F. Device Breakdown (Mobile vs Desktop)
        sql_device = f"""
            SELECT 
                if(match(browser, '(?i)(Mobile|Android|iPhone|iPad)'), 'Mobile', 'Desktop') as device,
                uniq(session_id) as c
            FROM analytics.user_activity WHERE date_num = {date_int} {hostname_filter}
            GROUP BY device
        """
        dev_res = ch.execute(sql_device)
        device_stats = {r[0]: r[1] for r in dev_res}

        # G. Top IPs (Potential Anomalies)
        sql_ips = f"""
            SELECT ip_user, count() as c
            FROM analytics.user_activity WHERE date_num = {date_int} {hostname_filter}
            GROUP BY ip_user ORDER BY c DESC LIMIT 10
        """
        ip_res = ch.execute(sql_ips)
        top_ips = [{"ip": r[0], "count": r[1]} for r in ip_res]

        # H. Engagement Stats (Quality Metrics)
        sql_engage = f"""
            SELECT 
                avg(duration) as avg_duration,
                countIf(events = 1) / count() as bounce_rate
            FROM (
                SELECT 
                    session_id, 
                    max(timestamp) - min(timestamp) as duration,
                    count() as events
                FROM analytics.user_activity 
                WHERE date_num = {date_int} {hostname_filter}
                GROUP BY session_id
            )
        """
        engage_res = ch.execute(sql_engage)
        engagement_stats = {"avg_duration": 0, "bounce_rate": 0}
        if engage_res:
            avg_dur = engage_res[0][0] if engage_res[0][0] is not None else 0
            b_rate = engage_res[0][1] if engage_res[0][1] is not None else 0
            engagement_stats = {
                "avg_duration": round(avg_dur, 1),
                "bounce_rate": round(b_rate * 100, 1)
            }

    finally:
        ch.disconnect()

    result = {
        "target_date": target_date,
        "summary_data": summary_data,
        "yesterday_data": yesterday_data,
        "last_week_data": last_week_data,
        "top_hosts": top_hosts,
        "hourly_trend": hourly_trend,
        "top_refs": top_refs,
        "top_urls": top_urls,
        "device_stats": device_stats,
        "top_ips": top_ips,
        "engagement_stats": engagement_stats,
        "is_today": is_today,
        "comparison_suffix": comparison_suffix,
        "last_updated": datetime.now(tz).strftime("%H:%M:%S %d/%m/%Y")
    }
    
    # --- REDIS CACHE SET ---
    if redis_client and should_cache:
        ttl = 604800 if is_past else 600 # 7 days for past, 10 mins for today
        try:
            redis_client.setex(cache_key, ttl, json.dumps(result))
        except Exception as e:
            print(f"[Redis Error] Set: {e}")
            
    return result

# --- Endpoint 1: Python Logic Report ---
@app.post("/report/analyze/python")
async def analyze_daily_python(body: AnalyzeRequest):
    ctx = fetch_daily_report_context(body.date, body.hostname)
    
    # Unpack context
    u = ctx["summary_data"].get('users', 0)
    b = ctx["summary_data"].get('bots', 0)
    v = ctx["summary_data"].get('views', 0)
    
    u_y = ctx["yesterday_data"].get('users', 0)
    b_y = ctx["yesterday_data"].get('bots', 0)
    v_y = ctx["yesterday_data"].get('views', 0)

    u_w = ctx["last_week_data"].get('users', 0)
    b_w = ctx["last_week_data"].get('bots', 0)
    v_w = ctx["last_week_data"].get('views', 0)
    
    is_today = ctx["is_today"]
    comparison_suffix = ctx["comparison_suffix"]
    device_stats = ctx["device_stats"]
    top_hosts = ctx["top_hosts"]
    top_refs = ctx["top_refs"]
    top_urls = ctx["top_urls"]
    hourly_trend = ctx["hourly_trend"]
    top_ips = ctx.get("top_ips", [])
    engage = ctx.get("engagement_stats", {"avg_duration": 0, "bounce_rate": 0})

    def calc_trend(curr, prev):
        if prev == 0: return "N/A" if curr == 0 else "🚀 +100%"
        change = ((curr - prev) / prev) * 100
        icon = "📈" if change > 0 else "📉"
        return f"{icon} {change:+.1f}%"
    
    label_y = "yesterday" if is_today else "previous day"
    label_w = "last week" if is_today else "same day last week"
    verdict = "Excellent 🚀" if u > 500000 else "Low 📉"
    
    top_str = "\n" + "\n".join([f"- **{h['host']}**: {h['users']:,} users" for h in top_hosts]) if top_hosts else "_No data_"
    ref_str = "\n" + "\n".join([f"- **{r['source']}**: {r['count']:,} sessions" for r in top_refs]) if top_refs else "_No data_"
    url_str = "\n" + "\n".join([f"- `{r['url'][:60]}...`: {r['count']:,} views" for r in top_urls]) if top_urls else "_No data_"
    ip_str = "\n" + "\n".join([f"- **{i['ip']}**: {i['count']:,} requests" for i in top_ips]) if top_ips else "_No data_"
    
    peak_info = f"around **{max(hourly_trend, key=hourly_trend.get)}:00**" if hourly_trend else "No data"

    report = f"""## 📊 Daily Traffic Evaluation (Python)

### 1. Executive Summary
- **Active Users**: {u:,} ({calc_trend(u, u_y)} vs {label_y}{comparison_suffix} | {calc_trend(u, u_w)} vs {label_w}{comparison_suffix})
- **Bot Sessions**: {b:,} ({calc_trend(b, b_y)} vs {label_y}{comparison_suffix} | {calc_trend(b, b_w)} vs {label_w}{comparison_suffix})
- **Total Page Views**: {v:,} ({calc_trend(v, v_y)} vs {label_y}{comparison_suffix} | {calc_trend(v, v_w)} vs {label_w}{comparison_suffix})
- **Devices**: {', '.join([f'{k}: {v:,}' for k, v in device_stats.items()]) if device_stats else 'N/A'}
- **Engagement**: Avg Duration {engage['avg_duration']}s | Bounce Rate {engage['bounce_rate']}%

### 2. Key Highlights
**🏆 Top Hostnames**
{top_str}

**🌍 Top Sources**
{ref_str}

**📄 Top Pages**
{url_str}

**🚨 Top Active IPs (Potential Bots)**
{ip_str}

### 3. Traffic Patterns
Peak traffic observed {peak_info}.

### 4. Verdict
**{verdict}** (Automated analysis based on user count)."""
    
    return {
        "report": report,
        "last_updated": ctx.get("last_updated"),
        "summary": {
            "active_users": u,
            "page_views": v,
            "bot_sessions": b
        },
        "chart_data": {
            "hourly": hourly_trend, 
            "top_hosts": top_hosts,
            "distribution": {"users": u, "bots": b},
            "devices": device_stats
        }
    }

# --- Endpoint 2: AI Report (Gemini) ---
@app.post("/report/analyze/ai")
async def analyze_daily_ai(body: AnalyzeRequest):
    ctx = fetch_daily_report_context(body.date, body.hostname)
    
    # Unpack context
    u = ctx["summary_data"].get('users', 0)
    b = ctx["summary_data"].get('bots', 0)
    v = ctx["summary_data"].get('views', 0)
    
    u_y = ctx["yesterday_data"].get('users', 0)
    u_w = ctx["last_week_data"].get('users', 0)
    
    target_date = ctx["target_date"]
    top_hosts = ctx["top_hosts"]
    top_refs = ctx["top_refs"]
    top_urls = ctx["top_urls"]
    hourly_trend = ctx["hourly_trend"]
    device_stats = ctx["device_stats"]
    comparison_suffix = ctx["comparison_suffix"]
    top_ips = ctx.get("top_ips", [])
    engage = ctx.get("engagement_stats", {"avg_duration": 0, "bounce_rate": 0})

    report = ""
    if GOOGLE_API_KEY:
        try:
            client = genai.Client(api_key=GOOGLE_API_KEY)
            prompt = f"""
            Role: Senior Strategic Data Advisor.
            Task: Analyze web traffic for {target_date} and provide strategic insights.
            
            Context Note: Comparison data (Yesterday/Last Week) is based on: "{'Same Time (Partial Data)' if comparison_suffix else 'Full Day (Completed Data)'}".
            
            Data Context:
            - **Traffic Overview**:
              - Active Users: {u} (Yesterday: {u_y}, Last Week: {u_w}) {comparison_suffix}
              - Page Views: {v}
              - Bot Sessions: {b}
            - **Engagement Quality**:
              - Average Visit Duration: {engage['avg_duration']} seconds
              - Bounce Rate: {engage['bounce_rate']}% (Percentage of single-page sessions)
            
            - **Top Hostnames**: {json.dumps(top_hosts)}
            - **Top Traffic Sources**: {json.dumps(top_refs)}
            - **Most Viewed Pages**: {json.dumps(top_urls)}
            - **Hourly Traffic Trend (Hour: UserCount)**: {json.dumps(hourly_trend)}
            - **Device Usage**: {json.dumps(device_stats)}
            - **Top Active IPs**: {json.dumps(top_ips)}
            
            Report Structure:
            1. **Executive Summary**: High-level overview of growth/decline and traffic quality (Engagement).
            2. **Deep Dive & Anomalies**: Analyze hourly trends, Top IPs, and Bot activity. Are there suspicious spikes? Is the Bounce Rate concerning?
            3. **Actionable Recommendations**: Suggest 3 specific actions (e.g., "Investigate Bot traffic on host X", "Optimize mobile experience", "Capitalize on source Y").
            4. **Forecast**: Briefly predict tomorrow's trend based on today's momentum.
            5. **Verdict**: Rating (Excellent/Stable/Concerning) with a strategic reason.
            
            Style: Professional but insightful. Don't just read numbers, explain *WHY*. Use emojis.
            """
            response = client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
            report = response.text
        except Exception as e:
            report = f"❌ **AI Analysis Failed**: {str(e)}"
    else:
        report = "⚠️ **AI Config Missing**: Please set `GOOGLE_API_KEY`."

    return {
        "report": report,
        "last_updated": ctx.get("last_updated"),
        "summary": {"active_users": u, "page_views": v, "bot_sessions": b},
        "chart_data": {
            "hourly": hourly_trend, 
            "top_hosts": top_hosts,
            "distribution": {"users": u, "bots": b},
            "devices": device_stats
        }
    }

# --- Endpoint 2b: Agent Report (New - Separated) ---
@app.post("/report/analyze/agent")
async def analyze_daily_agent(body: AnalyzeRequest):
    # 1. Lấy context dữ liệu (dùng chung logic fetch data)
    ctx = fetch_daily_report_context(body.date, body.hostname)
    
    # 2. Lấy dữ liệu CSV chi tiết
    csv_data = fetch_security_csv_context(body.date, body.hostname)
    
    # 3. Gọi sang module Agent (Truyền thêm CSV để AI đọc)
    report = await agent_client.analyze_traffic(ctx, csv_data)

    return {
        "report": report,
        "csv_data": csv_data,
        "last_updated": ctx.get("last_updated"),
        "summary": ctx["summary_data"],
        "chart_data": {
            "hourly": ctx["hourly_trend"], 
            "top_hosts": ctx["top_hosts"],
            "devices": ctx["device_stats"]
        }
    }

# --- Helper: Send Data to Bitrix ---
def send_bitrix_message(message: str):
    url = "https://monday.com.se/rest/11/cc951kcb4cajxd73/imbot.message.add.json"
    payload = {
        "BOT_ID": "226",
        "CLIENT_ID": "0m6pkxtvqzk4vwm7vey3q89ikczhrbrf",
        "DIALOG_ID": "chat2903",
        "MESSAGE": message
    }
    try:
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"⚠️ Bitrix Error: {e}")

# --- Helper: Fetch Security Data as CSV ---
def fetch_security_csv_context(date_input: str = None, hostname_input: str = None):
    tz = pytz.timezone("Asia/Manila")
    
    # Xử lý trường hợp date_input là "string" (do Swagger default) hoặc "undefined"
    if date_input and (date_input.lower() == "string" or date_input.lower() == "undefined"):
        date_input = None

    target_date = date_input if date_input else datetime.now(tz).strftime("%Y-%m-%d")
    try:
        date_int = int(target_date.replace("-", ""))
    except ValueError:
        # Fallback về hôm nay nếu format sai
        target_date = datetime.now(tz).strftime("%Y-%m-%d")
        date_int = int(target_date.replace("-", ""))
    
    hostname_filter = ""
    if hostname_input and hostname_input.strip():
        hostname_filter = f"AND hostname = '{hostname_input.strip()}'"

    # Logic lọc thời gian: Nếu không có date_input (tức là check cho hôm nay),
    # chỉ lấy dữ liệu trong 2 giờ gần nhất để tránh trùng lặp khi chạy định kỳ.
    time_filter = ""
    is_auto_run = False
    if not date_input:
        is_auto_run = True
        # Fix timezone: Chuyển timestamp sang Manila trước khi so sánh để chính xác
        time_filter = "AND toDateTime(timestamp, 'Asia/Manila') >= subtractHours(now('Asia/Manila'), 2)"

    # Query: Group by IP để tìm đối tượng khả nghi (Multi-host, Spam, Dirty Referer, Attacks)
    # Lấy Top 1000 user hoạt động nhiều nhất để check
    
    ch = get_clickhouse_client()

    def execute_security_query(d_int, t_filter):
        query = f"""
        SELECT
            ip_user,
            uniq(user_cookie) as distinct_cookies,
            any(browser) as user_agent,
            count() as total_requests,
            uniq(hostname) as distinct_hostnames,
            countIf(match(url, '(?i)(/admin|wp-admin|\\.env|\\.git|\\.bak|/config|phpinfo|\\.sql|\\.dump)')) as sensitive_hits,
            countIf(match(url, '(?i)(union%20select|<script>|alert\\(|eval\\(|document\\.cookie|\\.\\./|etc/passwd|1=1)')) as attack_hits,
            argMax(referer, timestamp) as sample_referer,
            sum(is_bot) as bot_flags,
            max(timestamp) - min(timestamp) as duration_sec,
              round(count() / if((max(timestamp) - min(timestamp)) > 0, (max(timestamp) - min(timestamp)), 1), 2) as rps
        FROM analytics.user_activity
        WHERE date_num = {d_int} {hostname_filter} {t_filter}
        GROUP BY ip_user
        ORDER BY (attack_hits * 50 + sensitive_hits * 20 + if(rps > 5, rps * 10, 0) + total_requests) DESC
        LIMIT 300
        """
        return ch.execute(query)

    try:
        # Lần 1: Chạy với bộ lọc thời gian (nếu có)
        rows = execute_security_query(date_int, time_filter)
        
        # Lần 2 (Fallback): Nếu chạy tự động (2h) mà không có data, thử lấy full ngày
        if not rows and is_auto_run:
            print("⚠️ No data in last 2h, falling back to full day scan...")
            rows = execute_security_query(date_int, "")

        # Lần 3 (Fallback): Nếu vẫn không có data, thử lấy ngày hôm qua (để demo/test không bị lỗi)
        if not rows and is_auto_run:
            print("⚠️ No data today, checking yesterday...")
            yesterday_int = int((datetime.now(tz) - timedelta(days=1)).strftime("%Y%m%d"))
            rows = execute_security_query(yesterday_int, "")
    finally:
        ch.disconnect()

    # Convert to CSV String
    output = io.StringIO()
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow(['ip', 'cookies', 'user_agent', 'reqs', 'hosts', 'sensitive', 'attacks', 'referer', 'bot_flags', 'duration', 'urls', 'rps'])
    
    for r in rows:
        # Cắt ngắn User Agent và Referer để tiết kiệm token cho AI
        ua_short = (r[2][:50] + '..') if len(r[2]) > 50 else r[2]
        # Đảm bảo xử lý an toàn nếu referer là None hoặc không phải string
        ref_val = str(r[7]) if r[7] else ""
        ref_short = (ref_val[:50] + '..') if len(ref_val) > 50 else ref_val
        writer.writerow([r[0], r[1], ua_short, r[3], r[4], r[5], r[6], ref_short, r[8], r[9], r[10], r[11]])
    
    return output.getvalue()

# --- Endpoint 2c: Security Analysis (CSV Input) ---
@app.post("/report/security/agent")
async def analyze_security_agent(body: AnalyzeRequest):
    csv_data = fetch_security_csv_context(body.date, body.hostname)
    print( f"[Security CSV] Rows: {len(csv_data.strip().splitlines()) - 1}" )
    
    # Xử lý label thời gian cho báo cáo
    tz = pytz.timezone("Asia/Manila")
    now = datetime.now(tz)
    date_label = body.date
    filename_suffix = body.date or 'today'

    if not date_label or date_label.lower() in ["string", "undefined"]:
        start_time = (now - timedelta(hours=2)).strftime("%H:%M")
        end_time = now.strftime("%H:%M")
        date_label = f"Today {start_time}-{end_time}"
        filename_suffix = f"today_{now.strftime('%H%M')}"

    # Kiểm tra nếu không có dữ liệu (chỉ có dòng header)
    if len(csv_data.strip().splitlines()) <= 1:
        return Response(
            content="verdict,score,ip,reason\nSafe,0,0.0.0.0,No traffic data found for this date",
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=security_analysis_{filename_suffix}.csv"}
        )

    report = await agent_client.analyze_security(csv_data)
    
    # Fallback: Nếu AI trả về rỗng hoặc lỗi, trả về file CSV chứa thông báo lỗi
    if not report or not report.strip():
        return Response(
            content="verdict,score,ip,reason\nError,0,0.0.0.0,AI returned empty response. Check Agent logs.",
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=security_analysis_error.csv"}
        )
    
    # Làm sạch response nếu AI trả về markdown block (ví dụ ```csv ... ```)
    clean_report = report.replace("```csv", "").replace("```", "").strip()
    
    # --- Gửi cảnh báo qua Bitrix ---
    try:
        # Parse CSV để lọc ra các IP nguy hiểm
        f = io.StringIO(clean_report)
        reader = csv.DictReader(f)
        
        alerts_map = {}
        for row in reader:
            # Xử lý key header có thể có khoảng trắng thừa
            r = {k.strip(): v for k, v in row.items() if k}
            verdict = r.get('verdict', '').lower()
            
            if verdict in ['critical', 'suspicious']:
                ip = r.get('ip', 'N/A')
                threat_type = r.get('threat_type', 'Unknown')

                # Filter out AI placeholders/templates (AI hallucination)
                if any(ph in ip for ph in ["<IP>", "&lt;IP&gt;", "[IP]"]) or \
                   any(ph in threat_type for ph in ["<threat_type>", "&lt;threat_type&gt;"]):
                    continue

                # Deduplicate: Ưu tiên Critical, nếu cùng mức độ thì giữ cái đầu tiên
                current_priority = 2 if verdict == 'critical' else 1
                
                if ip not in alerts_map or current_priority > alerts_map[ip]['priority']:
                    alerts_map[ip] = {'priority': current_priority, 'data': r}

        # Sort alerts by score (Critical first)
        sorted_alerts = sorted(alerts_map.values(), key=lambda x: x['priority'], reverse=True)
        
        alerts = []
        for item in sorted_alerts:
            r = item['data']
            verdict = r.get('verdict', '').lower()
            icon = "🔴" if verdict == 'critical' else "⚠️"
            
            country = r.get('country') or 'UNK'
            isp = r.get('isp') or 'Unknown'
            reason = r.get('reason') or 'N/A'
            solution = r.get('solution') or 'Check logs manually'
            score = r.get('score', 'N/A')
            alerts.append(f"{icon} **{r.get('ip', 'N/A')}** (Score: {score} | {r.get('threat_type', 'Unknown')})\n   🌍 {country} | {isp}\n   📝 {reason}\n   🛠️ {solution}")
        
        if alerts:
            msg = f"🚨 **Security Alert** ({date_label})\n\n" + "\n\n".join(alerts)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, send_bitrix_message, msg)
            
    except Exception as e:
        print(f"⚠️ Bitrix Notification Failed: {e}")

    return Response(
        content=clean_report,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=security_analysis_{filename_suffix}.csv"}
    )

# --- Endpoint 2d: Behavioral Analysis (New Feature) ---
@app.post("/report/behavior/agent")
async def analyze_behavior_agent(body: AnalyzeRequest):
    # 1. Lấy dữ liệu chuỗi hành vi (Session Journey)
    tz = pytz.timezone("Asia/Manila")
    target_date = body.date if body.date else datetime.now(tz).strftime("%Y-%m-%d")
    date_int = int(target_date.replace("-", ""))
    
    hostname_filter = ""
    if body.hostname and body.hostname.strip():
        hostname_filter = f"AND hostname = '{body.hostname.strip()}'"

    # Query: Gom nhóm URL theo cookie để tạo thành chuỗi "Path"
    # Chỉ lấy các cookie có ít nhất 2 bước di chuyển để phân tích
    query = f"""
    SELECT
        user_cookie, -- Dùng user_cookie thay vì session_id vì session_id đổi liên tục
        any(ip_user) as ip,
        substring(any(browser), 1, 50) as ua_short,
        count() as steps,
        max(timestamp) - min(timestamp) as duration,
        arrayStringConcat(arrayMap(x -> x.1, arraySort(x -> x.2, groupArray((url, timestamp)))), ' -> ') as path
    FROM analytics.user_activity
    WHERE date_num = {date_int} {hostname_filter}
    GROUP BY user_cookie
    HAVING steps >= 2 
    ORDER BY duration ASC -- Ưu tiên check các session nhanh bất thường trước
    LIMIT 100
    """
    
    ch = get_clickhouse_client()
    try:
        rows = ch.execute(query)
    finally:
        ch.disconnect()

    if not rows:
        return {"message": "No sufficient session data for behavioral analysis."}

    # Convert to CSV
    output = io.StringIO()
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow(['user_cookie', 'ip', 'ua', 'steps', 'duration', 'path'])
    
    for r in rows:
        # Cắt ngắn path nếu quá dài để tiết kiệm token
        path_str = r[5]
        if len(path_str) > 500:
            path_str = path_str[:500] + "..."
        writer.writerow([r[0], r[1], r[2], r[3], r[4], path_str])
    
    csv_data = output.getvalue()

    # 2. Gửi cho AI phân tích
    report = await agent_client.analyze_behavior(csv_data)
    
    return {
        "report": report,
        "analyzed_sessions": len(rows),
        "csv_preview": csv_data[:500] + "..."
    }

# --- Endpoint 3: Chat with Data (Ask AI) ---
class AskRequest(BaseModel):
    date: str
    hostname: str = None
    question: str

@app.post("/report/ask")
async def ask_ai_endpoint(body: AskRequest):
    # 1. Lấy lại context dữ liệu (Rất nhanh nhờ Redis Cache)
    ctx = fetch_daily_report_context(body.date, body.hostname)
    
    if not GOOGLE_API_KEY:
        return {"answer": "⚠️ AI Config Missing. Please set GOOGLE_API_KEY."}

    try:
        client = genai.Client(api_key=GOOGLE_API_KEY)
        
        # 2. Tạo prompt ngữ cảnh rút gọn cho Chat
        prompt = f"""
        Role: Friendly Data Assistant.
        Task: Answer the user's question based ONLY on the provided web traffic data for {ctx['target_date']}.
        
        Data Context:
        - Summary: {ctx['summary_data']}
        - Comparison: {ctx['comparison_suffix']} (Yesterday: {ctx['yesterday_data']}, Last Week: {ctx['last_week_data']})
        - Top Hostnames: {json.dumps(ctx['top_hosts'])}
        - Top Sources: {json.dumps(ctx['top_refs'])}
        - Top Pages: {json.dumps(ctx['top_urls'])}
        - Hourly Trend: {json.dumps(ctx['hourly_trend'])}
        - Devices: {json.dumps(ctx['device_stats'])}
        - Top IPs: {json.dumps(ctx.get('top_ips', []))}
        - Engagement: {json.dumps(ctx.get('engagement_stats', {}))}
        
        User Question: "{body.question}"
        
        Answer:
        - Be direct and concise.
        - Use data points to back up your answer.
        - If the data doesn't support the answer, say "I don't have enough data to answer that."
        - Use Markdown for formatting (bold, list).
        """
        
        response = client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
        return {"answer": response.text}
        
    except Exception as e:
        return {"answer": f"❌ Error: {str(e)}"}

# --- Endpoint 3b: Chat with Agent (New - Separated) ---
@app.post("/report/ask/agent")
async def ask_agent_endpoint(body: AskRequest):
    # 1. Lấy context
    ctx = fetch_daily_report_context(body.date, body.hostname)
    
    # 2. Gọi module Agent
    answer = await agent_client.chat_with_data(ctx, body.question)
    return {"answer": answer}

# --- Endpoint 3: Clear Cache ---
@app.post("/report/clear-cache")
async def clear_report_cache(body: AnalyzeRequest):
    tz = pytz.timezone("Asia/Manila")
    now_manila = datetime.now(tz)
    
    if body.date:
        target_date = body.date
    else:
        target_date = now_manila.strftime("%Y-%m-%d")

    date_int = int(target_date.replace("-", ""))
    hostname_input = body.hostname
    
    cache_key = f"report_ctx:{date_int}:{hostname_input.strip() if hostname_input else 'all'}"
    
    if not redis_client:
        return {"status": "error", "message": "Redis is not configured."}

    try:
        redis_client.delete(cache_key)
        return {"status": "ok", "message": f"Cache cleared for {target_date}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/dashboard/report", response_class=HTMLResponse)
def report_dashboard_ui(request: Request):
    return templates.TemplateResponse("daily_report.html", {"request": request, "active_page": "report"})

# --- Endpoint: Security Dashboard (Show Redis Alerts) ---
@app.get("/dashboard/security", response_class=HTMLResponse)
async def security_dashboard_ui(request: Request, page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=100), search: str = Query(None)):
    if not redis_client:
        return templates.TemplateResponse("security_alerts.html", {"request": request, "alerts": [], "error": "Redis is not configured.", "page": 1, "total_pages": 1, "search": search})
    
    alerts = []
    try:
        # Scan tất cả các key security_alert:*
        keys = []
        # Sử dụng scan_iter để an toàn với memory nếu có quá nhiều key
        for k in redis_client.scan_iter("security_alert:*"):
            keys.append(k)
        
        if keys:
            # Lấy value hàng loạt (Pipeline/MGET) - Chunking để tránh lỗi nếu quá nhiều key
            chunk_size = 1000
            for i in range(0, len(keys), chunk_size):
                chunk = keys[i:i+chunk_size]
                values = redis_client.mget(chunk)
                for val in values:
                    if val:
                        try:
                            data = json.loads(val)
                            alerts.append(data)
                        except json.JSONDecodeError:
                            pass
        
        # Filter by Search Term (IP, Country, ISP, Threat Type)
        if search:
            search_lower = search.lower()
            alerts = [
                a for a in alerts 
                if search_lower in str(a.get('ip', '')).lower() or 
                   search_lower in str(a.get('country', '')).lower() or 
                   search_lower in str(a.get('isp', '')).lower() or
                   search_lower in str(a.get('threat_type', '')).lower()
            ]

        # Sắp xếp theo thời gian mới nhất
        alerts.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
        
        # Phân trang (Pagination)
        total_records = len(alerts)
        total_pages = math.ceil(total_records / limit) if limit > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paged_alerts = alerts[start_idx:end_idx]

        # Xử lý hiển thị Reason: Cắt ngắn nếu quá dài để UI gọn hơn, hover xem full
        for alert in paged_alerts:
            r = alert.get('reason', '')
            if len(r) > 80:
                alert['reason_display'] = r[:80] + '...'
                alert['reason_full'] = r
            else:
                alert['reason_display'] = r
                alert['reason_full'] = r
        
    except Exception as e:
        return templates.TemplateResponse("security_alerts.html", {"request": request, "alerts": [], "error": str(e), "page": 1, "total_pages": 1, "search": search})

    return templates.TemplateResponse("security_alerts.html", {
        "request": request, 
        "alerts": paged_alerts, 
        "active_page": "security",
        "page": page,
        "total_pages": total_pages,
        "limit": limit,
        "search": search
    })

# --- Endpoint: Clear IP from Cache (Dismiss Alert) ---
class ClearIpRequest(BaseModel):
    ips: List[str]

@app.post("/api/clear-ip")
async def clear_ip_cache(body: ClearIpRequest):
    if not redis_client:
        return JSONResponse({"status": "error", "message": "Redis is not configured"}, status_code=500)

    try:
        deleted_count = 0
        for ip in body.ips:
            keys = list(redis_client.scan_iter(f"security_alert:{ip}:*"))
            if keys:
                redis_client.delete(*keys)
                deleted_count += len(keys)
        return {"status": "ok", "message": f"✅ Cleared {deleted_count} alerts for {len(body.ips)} IPs."}
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- Endpoint: Block IP on Cloudflare ---
class BlockIpRequest(BaseModel):
    ips: List[str]
    reason: str = "Blocked via Security Dashboard"

@app.post("/api/block-ip")
async def block_ip_cloudflare(body: BlockIpRequest):
    if not CLOUDFLARE_LIST_TOKEN or not CLOUDFLARE_IPV4_LIST_URL or not CLOUDFLARE_IPV6_LIST_URL:
        return JSONResponse({"status": "error", "message": "Cloudflare configuration missing"}, status_code=500)

    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_LIST_TOKEN}",
        "Content-Type": "application/json"
    }
    
    ipv4_items = []
    ipv6_items = []
    valid_ips = []

    # Phân loại IP
    for ip in body.ips:
        try:
            ip_obj = ipaddress.ip_address(ip)
            if ip_obj.version == 4:
                item = {"ip": ip, "comment": body.reason}
                ipv4_items.append(item)
            elif ip_obj.version == 6:
                # Cloudflare requires IPv6 CIDR /64
                ipv6_cidr = str(ipaddress.IPv6Network(f"{ip}/64", strict=False))
                item = {"ip": ipv6_cidr, "comment": body.reason}
                ipv6_items.append(item)
            valid_ips.append(ip)
        except ValueError:
            continue

    try:
        loop = asyncio.get_event_loop()
        success_count = 0
        errors = []

        async def send_batch(url, items):
            if not items: return True, None
            # Cloudflare Lists API nhận body là mảng items: [{"ip": "...", "comment": "..."}]
            payload = items
            response = await loop.run_in_executor(None, lambda: requests.post(url, json=payload, headers=headers, timeout=10))
            
            try:
                data = response.json()
                if response.status_code == 200 and data.get("success"):
                    return True, None
                return False, f"Status: {response.status_code}, Error: {data.get('errors', response.text)}"
            except:
                return False, f"Status: {response.status_code}, Body: {response.text}"

        # Gửi batch IPv4
        if ipv4_items:
            ok, err = await send_batch(CLOUDFLARE_IPV4_LIST_URL, ipv4_items)
            if ok:
                success_count += len(ipv4_items)
            else:
                errors.append(f"IPv4 Error: {err}")

        # Gửi batch IPv6
        if ipv6_items:
            ok, err = await send_batch(CLOUDFLARE_IPV6_LIST_URL, ipv6_items)
            if ok:
                success_count += len(ipv6_items)
            else:
                errors.append(f"IPv6 Error: {err}")
        
        # --- Xóa alert khỏi Redis cho các IP đã xử lý thành công (hoặc tất cả valid_ips nếu muốn clean up) ---
        # Ở đây ta xóa cho tất cả valid_ips nếu không có lỗi nghiêm trọng, để tránh spam alert
        if redis_client and success_count > 0:
            keys_to_delete = []
            for ip in valid_ips:
                for k in redis_client.scan_iter(f"security_alert:{ip}:*"):
                    keys_to_delete.append(k)
            
            if keys_to_delete:
                redis_client.delete(*keys_to_delete)

        if not errors:
            return {"status": "ok", "message": f"✅ Successfully blocked {success_count} IPs on Cloudflare."}
        elif success_count > 0:
             return {"status": "partial", "message": f"⚠️ Blocked {success_count}/{len(body.ips)} IPs. Errors: {errors}"}
        else:
            return JSONResponse({"status": "error", "message": "Failed to block IPs", "details": errors}, status_code=400)
            
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
