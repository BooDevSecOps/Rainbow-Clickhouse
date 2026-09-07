(function() {
    function checkAdvancedBot() {
        const nav = window.navigator;
        if (nav.webdriver) return 1;
        if (/HeadlessChrome/.test(nav.userAgent) || !nav.languages || nav.languages.length === 0) return 1;
        if (nav.plugins.length === 0) return 1;
        if (window.screen.width === 0 || window.screen.height === 0) return 1;
        return 0;
    }

    function getUserCookieCH() {
        const cookieNameCH = "user_cookie_ch";
        const existing = document.cookie
            .split("; ")
            .find(row => row.startsWith(cookieNameCH + "="));
        
        if (existing) return existing.split("=")[1];

        const value = crypto.randomUUID();
        const expire = new Date();
        expire.setDate(expire.getDate() + 90);
        document.cookie = `${cookieNameCH}=${value}; expires=${expire.toUTCString()}; path=/; SameSite=Lax`;
        return value;
    }

    const userCookieCH = getUserCookieCH();
    const sessionId = crypto.randomUUID();
    const isBot = checkAdvancedBot();
    let heartbeatInterval = null;

   
    function sendHeartbeatCH(isExiting = false) {
        if (document.visibilityState !== 'visible' && !isExiting) return;

        const payload = {
            session_id: sessionId,
            user_cookie: userCookieCH,
            url: window.location.href,
            hostname: window.location.hostname,
            referer: document.referrer,
            browser: navigator.userAgent, 
            is_bot: isBot
        };

        fetch("https://dashboard.mbaku.org/track", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
            keepalive: true
        })
        .then(res => res.ok && !isExiting && console.log("[Analytics] Synced"))
        .catch(err => !isExiting && console.error("[Analytics] Error:", err));
    }

    function startTracking() {
        sendHeartbeatCH(); 
        if (heartbeatInterval) clearInterval(heartbeatInterval);
        heartbeatInterval = setInterval(sendHeartbeatCH, 10000); 
    }

    function stopTracking() {
        if (heartbeatInterval) clearInterval(heartbeatInterval);
    }

    document.addEventListener("visibilitychange", function() {
        if (document.visibilityState === 'hidden') {
            sendHeartbeatCH(true); 
            stopTracking();
        } else {
            startTracking(); 
        }
    });

    startTracking();
})();