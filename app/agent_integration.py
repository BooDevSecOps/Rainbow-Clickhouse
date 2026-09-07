import json
try:
    from openai import AsyncOpenAI
except ImportError:
    AsyncOpenAI = None

class AgentIntegration:
    def __init__(self, base_url: str, api_key: str, model: str):
        if AsyncOpenAI:
            self.client = AsyncOpenAI(
                base_url=base_url,
                api_key=api_key,
            )
        else:
            self.client = None
        self.model = model

    async def analyze_traffic(self, ctx: dict, csv_data: str = None) -> str:
        """
        Gửi dữ liệu traffic sang Agent để phân tích báo cáo hàng ngày.
        """
        if not self.client:
            return "❌ **System Error**: `openai` library is not installed. Please run `pip install openai`."

        # 1. Trích xuất dữ liệu từ context (giống logic cũ nhưng xử lý nội bộ tại đây)
        u = ctx["summary_data"].get('users', 0)
        b = ctx["summary_data"].get('bots', 0)
        v = ctx["summary_data"].get('views', 0)
        
        u_y = ctx["yesterday_data"].get('users', 0)
        u_w = ctx["last_week_data"].get('users', 0)
        
        comparison_suffix = ctx.get("comparison_suffix", "")
        engage = ctx.get("engagement_stats", {"avg_duration": 0, "bounce_rate": 0})

        # 2. Xây dựng Prompt chuyên biệt cho Agent
        system_prompt = "You are a Senior Strategic Data Advisor. You analyze web traffic data and provide actionable insights."
        
        user_prompt = f"""
        Task: Analyze web traffic for {ctx.get('target_date')} and provide strategic insights.
        
        Context Note: Comparison data is based on: "{'Same Time (Partial Data)' if comparison_suffix else 'Full Day (Completed Data)'}".
        
        Data Context:
        - **Traffic Overview**:
          - Active Users: {u} (Yesterday: {u_y}, Last Week: {u_w}) {comparison_suffix}
          - Page Views: {v}
          - Bot Sessions: {b}
        - **Engagement Quality**:
          - Average Visit Duration: {engage['avg_duration']} seconds
          - Bounce Rate: {engage['bounce_rate']}%
        
        - **Top Hostnames**: {json.dumps(ctx.get('top_hosts', []))}
        - **Top Traffic Sources**: {json.dumps(ctx.get('top_refs', []))}
        - **Most Viewed Pages**: {json.dumps(ctx.get('top_urls', []))}
        - **Hourly Traffic Trend**: {json.dumps(ctx.get('hourly_trend', {}))}
        - **Device Usage**: {json.dumps(ctx.get('device_stats', {}))}
        - **Top Active IPs**: {json.dumps(ctx.get('top_ips', []))}
        
        {f'''**Detailed Traffic Sample (CSV)**:
        Columns: ip, country, isp, cookies, user_agent, reqs, hosts, sensitive, attacks, referer, bot_flags, duration.
        Use this data to identify specific suspicious IPs (multi-host, dirty referer), bots, or scraping patterns in the "Deep Dive" section.
        ```csv
        {csv_data}
        ```''' if csv_data else ''}
        
        Report Structure:
        1. **Executive Summary**: High-level overview.
        2. **Deep Dive & Anomalies**: Analyze trends, IPs, Bots.
        3. **Actionable Recommendations**: Suggest 3 specific actions.
        4. **Forecast**: Predict tomorrow's trend.
        5. **Verdict**: Rating (Excellent/Stable/Concerning).
        
        Style: Professional but insightful. Use emojis.
        """

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": f"{system_prompt}\n\n{user_prompt}"}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"❌ **Agent Analysis Failed**: {str(e)}"

    async def chat_with_data(self, ctx: dict, question: str) -> str:
        """
        Chat với Agent dựa trên dữ liệu context đã có.
        """
        if not self.client:
            return "❌ **System Error**: `openai` library is not installed. Please run `pip install openai`."

        data_context = f"""
        Data Context:
        - Summary: {ctx['summary_data']}
        - Comparison: {ctx['comparison_suffix']} (Yesterday: {ctx['yesterday_data']}, Last Week: {ctx['last_week_data']})
        - Top Hostnames: {json.dumps(ctx.get('top_hosts', []))}
        - Top Sources: {json.dumps(ctx.get('top_refs', []))}
        - Top Pages: {json.dumps(ctx.get('top_urls', []))}
        - Hourly Trend: {json.dumps(ctx.get('hourly_trend', {}))}
        - Devices: {json.dumps(ctx.get('device_stats', {}))}
        - Top IPs: {json.dumps(ctx.get('top_ips', []))}
        - Engagement: {json.dumps(ctx.get('engagement_stats', {}))}
        """

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": f"Role: Friendly Data Assistant.\nTask: Answer based ONLY on provided data.\n{data_context}\n\nUser Question: {question}"}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"❌ **Agent Chat Failed**: {str(e)}"

    async def analyze_security(self, csv_data: str) -> str:
        """
        Gửi file CSV (dạng text) chứa thông tin user/cookie để AI check spam/bot.
        """
        if not self.client:
            return "❌ **System Error**: `openai` library is not installed."

        # Escape ký tự { và } để tránh lỗi f-string nếu URL/UA chứa JSON hoặc code độc hại
        safe_csv_data = csv_data.replace("{", "{{").replace("}", "}}")
                # - **Scraping**: High `reqs` with high `rps` or low `distinct_urls` (hammering same URL), even if UA is valid.

        system_prompt = "You are a Security Log Analyzer. Output ONLY CSV data. Do not output reasoning steps. No intro, no outro."
        
        user_prompt = f"""
        Analyze this traffic log (grouped by IP) to find threats.
        Columns: ip, (optional: country, isp), cookies, user_agent, reqs, hosts, sensitive (admin/config hits), attacks (sqli/xss hits), referer, bot_flags, duration, distinct_urls, rps.
        
        **Context**: Normal user behavior typically involves `cookies` > 0, `referer` from search/social/direct, and `duration` > 10s.
        
        **1. Rule-Based Detection (Baseline)**:
        - **Critical**: `attacks` > 0 OR `hosts` > 5 OR `sensitive` > 5 OR `referer` matches (porn/gamble/viagra/sex).
        - **Suspicious**: `cookies` > 15 OR UA matches (python/curl/http/headless) OR (`reqs` > 500 AND `duration` < 10) OR `rps` > 50.
        
        **2. AI Behavioral Analysis (Auto-Detection)**:
        - **Instruction**: Use your security intuition to find **Anomalies** that don't fit the strict rules.
        - **Look for**:
          - **Spoofing**: User-Agents claiming to be standard browsers (Chrome/Safari) but have **0 cookies** or **empty referer** combined with **high rps** (> 2.0).
          - **Low-and-Slow Probing**: Low `reqs` (< 10) but `sensitive` > 0 (trying to find admin panels quietly).
          - **Botnet**: If multiple IPs share the exact same unusual User-Agent or behavior pattern.
          - **Geo/ISP Anomalies**: High traffic from Data Center ISPs (e.g., DigitalOcean, AWS, Hetzner) or unusual countries. **(You must infer Country/ISP from IP)**.
        
        **Constraints**:
        - **Strictly use provided data**: Do not invent IPs or metrics not present in the CSV.
        - **Consistency**: If an IP is marked Critical/Suspicious, do not list it again as Safe.

        **Task**:
        - Identify ALL Critical/Suspicious IPs (combine Rules + AI Analysis).
        - **IMPORTANT**: Output ONLY IPs with verdict 'Critical' or 'Suspicious'. Do NOT output 'Safe' IPs or 'Normal traffic'.
        - If no threats are found, output ONLY the header.
        - Output format: `verdict,score,ip,country,isp,threat_type,reason,solution`
        - **Requirement**: Use provided Country/ISP columns if available. If missing, you MUST estimate/lookup them based on IP.
        - **Score**: Risk score from 0 (Safe) to 100 (Critical).
        - **Reason**: Provide the reason in both English and Vietnamese. Format: "English reason. (Lý do tiếng Việt)". **MUST include specific numbers/metrics** from the data (e.g., "High cookie count (45)...", "RPS 120.5 exceeds limit...").
        
        **Example Output**:
        verdict,score,ip,country,isp,threat_type,reason,solution
        Critical,95,192.168.1.5,US,DigitalOcean,Dirty Referer,"Referer: 'http://casino-online.com' contains 'casino'. (Referer chứa từ khóa cờ bạc)","Block IP and Referer domain"
        Critical,90,45.33.22.11,CN,Chinanet,Web Attack,"Detected 5 SQLi/XSS attempts in URL. (Phát hiện 5 lần thử tấn công SQLi/XSS trong URL)","Block IP immediately"
        Suspicious,65,10.0.0.5,VN,Viettel,AI Anomaly,"Mobile UA accessing /admin with 0 cookies. (User-Agent mobile truy cập /admin mà không có cookie)","Monitor for 1 hour"
        Suspicious,50,1.2.3.4,VN,VNPT,Cookie Abuse,"High cookie count (25) indicating potential bot. (Số lượng cookie cao (25) cho thấy có thể là bot)","Rate limit"
        
        **CSV Data**:
        ```csv
        {safe_csv_data}
        ```
        """

        try:
            print(f"🚀 Sending request to Agent... (CSV Length: {len(csv_data)})")
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": f"{system_prompt}\n\n{user_prompt}"}
                ],
                max_tokens=16000 # Tăng token limit để tránh bị cắt khi model suy luận quá dài
            )
            
            # --- DEBUGGING CHI TIẾT ---
            # print(f"🔍 [Agent Response Object]: {response}")
            
            if not response.choices:
                print("❌ [Agent Error] No choices returned.")
                return "verdict,ip,threat_type,reason\nError,0.0.0.0,System Error,No choices returned from Agent"
                
            choice = response.choices[0]
            content = choice.message.content
            finish_reason = choice.finish_reason
            
            print(f"ℹ️ [Agent Finish Reason]: {finish_reason}")

            # Check for refusal
            if getattr(choice.message, 'refusal', None):
                return f"verdict,ip,threat_type,reason\nError,0.0.0.0,Refusal,{choice.message.refusal}"
            
            if not content:
                # Fallback: Check if model spent all tokens reasoning (common with Reasoning models)
                reasoning = getattr(choice.message, 'reasoning_content', None)
                if reasoning:
                    print(f"⚠️ [Agent Warning] Content empty. Reasoning found (len={len(reasoning)}). Model likely hit token limit.")
                    return f"verdict,ip,threat_type,reason\nError,0.0.0.0,Token Limit Exceeded,See reasoning below\n\n# REASONING LOG (Analysis before cutoff):\n{reasoning}"
                
                print("⚠️ [Agent Warning] Content is empty and no reasoning found.")
                return "verdict,ip,threat_type,reason\nError,0.0.0.0,Empty Response,Agent returned no content."
                
            print(f"✅ [Agent Content Preview]: {content[:200]}...") 
            return content

        except Exception as e:
            print(f"❌ [Agent Exception]: {str(e)}")
            return f"❌ **Security Analysis Failed**: {str(e)}"

    async def analyze_behavior(self, csv_data: str) -> str:
        """
        Phân tích hành vi di chuyển (Navigation Path) của user_cookie.
        """
        if not self.client:
            return "❌ **System Error**: `openai` library is not installed."

        system_prompt = "You are a Behavioral Analyst. Analyze user navigation paths to detect non-human patterns."
        
        user_prompt = f"""
        Analyze these web session logs to find Bots/Scrapers based on their navigation path.
        
        **Data Columns**: 
        - `user_cookie`: Unique user identifier.
        - `ip`: User IP.
        - `ua`: User Agent (shortened).
        - `steps`: Number of pages visited.
        - `duration`: Total time spent (seconds).
        - `path`: Sequence of URLs visited (connected by '->').

        **Detection Logic**:
        1. **Machine Speed**: High `steps` (> 3) but very low `duration` (< 2s).
        2. **Teleporting**: Jumping between unrelated pages without logical flow (e.g., /contact -> /admin -> /checkout).
        3. **Looping**: Repeating the same URL sequence multiple times.
        4. **Direct Hunting**: Path starts immediately with sensitive pages (/login, /admin) without landing page.

        **Task**:
        - Identify suspicious sessions.
        - Output format: `verdict,ip,user_cookie,reason,solution`
        - If no threats found, output ONLY the header.

        **CSV Data**:
        ```csv
        {csv_data}
        ```
        """

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": f"{system_prompt}\n\n{user_prompt}"}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"❌ **Behavior Analysis Failed**: {str(e)}"

    async def analyze_pattern(self, csv_data: str) -> str:
        """
        Phân tích các nhóm URL/Path có lưu lượng cao để phát hiện tấn công tập trung (DDoS L7, Scraping, Mass Scan).
        """
        if not self.client:
            return "❌ **System Error**: `openai` library is not installed."

        system_prompt = "You are a Threat Intelligence Analyst. Analyze traffic clusters to detect coordinated attacks."
        
        user_prompt = f"""
        Analyze these traffic clusters (grouped by URL Path) to find coordinated attacks or anomalies.
        
        **Data Columns**: 
        - `hostname`: Target domain.
        - `path`: URL path being accessed.
        - `distinct_ips`: Number of unique IPs accessing this path.
        - `reqs`: Total requests.
        - `distinct_uas`: Number of unique User-Agents.
        - `distinct_refs`: Number of unique Referers.
        - `ua_sample`: Sample User-Agent.
        - `ref_sample`: Sample Referer.

        **Detection Logic**:
        1. **DDoS / Botnet**: High `distinct_ips` but LOW `distinct_uas` (e.g., 1000 IPs but only 1-5 UAs) OR Empty Referers.
        2. **Mass Targeting**: High traffic on sensitive paths (/admin, /login, /api/v1/users) regardless of UA diversity.
        3. **Viral / Live Stream (SAFE)**: High `distinct_ips` AND High `distinct_uas` (> 20% of IPs) AND URL looks like content (news, video, stream, product, truc-tiep). **Verdict: Safe**.
        4. **Normal Traffic (SAFE)**: Homepage (/) or static assets (.css, .js, .jpg).

        **Task**:
        - Identify clusters that look like **Attacks** or **Abuse**.
        - **Ignore** Viral Content, Live Streams, and Homepage traffic (High UA diversity).
        - Output format: `verdict,target,ip_count,threat_type,reason,solution`
        - If no threats found, output ONLY the header.

        **CSV Data**:
        ```csv
        {csv_data}
        ```
        """

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": f"{system_prompt}\n\n{user_prompt}"}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"❌ **Pattern Analysis Failed**: {str(e)}"

    async def analyze_referer(self, csv_data: str) -> str:
        """
        Phân tích danh sách Referer để phát hiện Referer Spam/Dirty.
        """
        if not self.client:
            return "❌ **System Error**: `openai` library is not installed."

        system_prompt = "You are a SEO & Security Analyst. Analyze web referers to detect Spam/Dirty traffic."
        
        user_prompt = f"""
        Analyze these top referers to detect "Dirty Referers" (Spam, Porn, Gambling, SEO Injection, Botnet Command) OR **Traffic Anomalies**.
        
        **Data Columns**: 
        - `referer`: The referring URL.
        - `domain`: Extracted domain.
        - `ips`: Unique IPs coming from this referer.
        - `reqs`: Total requests.
        - `targets`: The hostname(s) on our side receiving traffic.
        - `title`: The HTML <title> of the referer page (Crawled).
        - `description`: The meta description of the referer page (Crawled).

        **Detection Logic**:
        1. **Referer Spam**: Domains like semalt.com, buttons-for-website.com, or cheap SEO services.
        2. **Adult/Gambling**: Porn sites, Casino, Sex, Betting sites sending traffic to non-related sites.
        3. **SEO Injection**: Referers containing keywords like "viagra", "cialis", "buy-now" in the query string.
        4. **Compromised / Hacked Sites (CRITICAL)**:
           - **Rule**: If a seemingly legitimate business domain (e.g., construction, shop, blog, industry) sends high traffic (> 500 reqs) to unrelated targets (e.g., a steel site linking to a vet/art site), it is **Compromised**.
           - **Verdict**: Mark as **Critical**.
           - **Reason**: "Likely hacked site hosting hidden spam/adult content (Referer Spaming)".
        5. **Referer Spoofing (SUSPICIOUS)**:
           - **Rule**: High traffic from major news/portal sites (e.g., msn.com, news sites) to unrelated niche sites.
           - **Verdict**: Mark as **Suspicious**.
           - **Reason**: "Likely Botnet spoofing legitimate domains in Referer header".
        6. **Content Mismatch (CRITICAL)**:
           - **Rule**: If `title` or `description` contains keywords like "Casino", "Sex", "Porn", "Betting", "Viagra" BUT the domain name sounds normal/business-like.
           - **Verdict**: Mark as **Critical**.
        7. **Safe**: Social media (Facebook, Instagram, TikTok, Twitter, LinkedIn, YouTube), **Legitimate Mainstream Websites** (News, Gov, Edu, Big Portals) sending high traffic (Viral/Backlink) are **SAFE**.

        **Task**:
        - Identify **Dirty/Spam** referers AND **Compromised Sites**.
        - Output format: `verdict,referer,reqs,targets,type,reason,action`
        - Verdicts: `Critical` (Malicious/Porn/Gambling/Botnet), `Suspicious` (SEO Spam/High Traffic), `Safe`.
        - **Output ONLY Critical or Suspicious items**.
        - If no threats found, output ONLY the header.

        **CSV Data**:
        ```csv
        {csv_data}
        ```
        """

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": f"{system_prompt}\n\n{user_prompt}"}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"❌ **Referer Analysis Failed**: {str(e)}"