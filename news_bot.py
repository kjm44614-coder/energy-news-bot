"""
에너지 뉴스 → 텔레그램 채널 자동 게시 봇

흐름: 소스별 RSS 수집 → 키워드/하위주제 분류 → 중복 제거(seen.json) → 텔레그램 전송
환경변수:
  TELEGRAM_BOT_TOKEN  (필수) BotFather에서 받은 토큰
  TELEGRAM_CHAT_ID    (필수) 채널 @username 또는 -100xxxxxxxxxx
  ANTHROPIC_API_KEY   (선택) 있으면 제목을 한국어로 번역해서 같이 게시
"""
import os
import re
import json
import time
import hashlib
import html
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import feedparser
import requests

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY")

SEEN_FILE = "seen.json"
LOOKBACK_HOURS = 3      # 이 시간보다 오래된 기사는 무시
MAX_PER_RUN = 15        # 1회 실행당 최대 게시 수 (첫 실행 폭주 방지)
SEND_INTERVAL = 30       # 메시지 간 대기(초) - 텔레그램 rate limit 대응
HEADERS = {"User-Agent": "Mozilla/5.0 (energy-news-bot)"}

# ---------------------------------------------------------------------------
# 1) 소스: 도메인 기반 (Google News RSS의 site: 검색을 이용 -> RSS 없는 매체도 커버)
#    공식 RSS 주소를 알고 있으면 DIRECT_FEEDS에 추가하면 더 정확함 (요약문 포함)
# ---------------------------------------------------------------------------
SOURCES = {
    "Reuters": "reuters.com",
    "Montel": "montelnews.com",
    "Energy Intelligence": "energyintel.com",
    "Argus": "argusmedia.com",
    "S&P Global": "spglobal.com",
    "Recharge": "rechargenews.com",
    "PV Magazine": "pv-magazine.com",
    "Canary Media": "canarymedia.com",
    "Energy Storage News": "energy-storage.news",
    "World Nuclear News": "world-nuclear-news.org",
    "NEI Magazine": "neimagazine.com",
    "T&D World": "tdworld.com",
    "Utility Dive": "utilitydive.com",
    "LNG Prime": "lngprime.com",
    "NGI": "naturalgasintel.com",
    "IEA": "iea.org",
    "Rystad": "rystadenergy.com",
    "Wood Mackenzie": "woodmac.com",
    "Nikkei Asia": "asia.nikkei.com",
}

DIRECT_FEEDS = {
    # "PV Magazine (RSS)": "https://www.pv-magazine.com/feed/",   # 예시: 직접 확인 후 추가
}


def google_news_url(domain: str) -> str:
    q = quote(f"site:{domain} when:{LOOKBACK_HOURS}h")
    return f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"


# ---------------------------------------------------------------------------
# 2) 키워드 분류: core(카테고리 진입 조건) + topics(하위 범위)
#    require_topic=True 면 하위 주제까지 맞아야 게시 (노이즈 큰 카테고리용)
# ---------------------------------------------------------------------------
CATEGORIES = {
    "AIDC/Datacenter": {
        "require_topic": True,
        "core": r"data ?cent(er|re)s?|\bAIDC\b|hyperscal|AI (campus|factory|factories)",
        "topics": {
            "딜/투자": r"\bPPA\b|power purchase|supply agreement|acquir|acquisition|merger|\bM&A\b|invest|billion|joint venture|stake|to build|unveil|announce",
            "규제/인허가": r"permit|zoning|moratorium|delay|opposition|residents|water|interconnection|grid connection|regulat|approval|\bban\b|policy",
            "운영 리스크": r"outage|shut ?down|curtail|blackout|grid (access|constraint)|power (shortage|constraint)|fire",
        },
    },
    "Solar": {
        "require_topic": False,
        "core": r"\bsolar\b|photovoltaic|\bPV\b|perovskite",
        "topics": {
            "공급망": r"polysilicon|wafer|\bcells?\b|module|price|capacity|curtail|cut(s|ting)? output|overcapacity",
            "정책/무역": r"tariff|subsid|tax credit|anti-?dumping|countervailing|Section 232|\bFEOC\b|\b45X\b|\bITC\b|duties|trade",
            "프로젝트": r"\bPPA\b|gigawatt|\bGW\b|\bMW\b|solar (farm|park|plant|project)|commission|groundbreaking|energi[sz]ed|online",
            "기술": r"efficiency|perovskite|TOPCon|\bHJT\b|tandem|back.?contact|record",
        },
    },
    "Wind": {
        "require_topic": False,
        "core": r"wind (farm|power|turbine|energy|project|park|auction)|offshore wind|onshore wind|Vestas|Siemens Gamesa|Ørsted|Orsted|GE Vernova|Goldwind|Mingyang|Nordex|Vineyard Wind",
        "topics": {
            "프로젝트": r"construction|commission|auction|tender|bid|energi[sz]ed|online|first power|installation",
            "규제": r"permit|approval|cancel|lease|subsid|stop.?work|revok|suspend|halt",
            "공급망": r"order|backlog|earnings|results|supply chain|blade|nacelle|component|vessel|cable",
            "기업동향": r"acquir|acquisition|merger|\bM&A\b|partnership|joint venture|stake|divest",
        },
    },
    "Fuelcell": {
        "require_topic": False,
        "core": r"fuel ?cells?|Bloom Energy|Plug Power|FuelCell Energy|Ballard|Doosan Fuel Cell|\bSOFC\b",
        "topics": {
            "딜/계약": r"data ?cent|contract|agreement|order|deploy|supply|megawatt|\bMW\b|\bGW\b",
            "기술/제품": r"launch|unveil|new (product|model)|efficiency|technology",
            "정책": r"subsid|hydrogen|policy|tax credit|\b45V\b|incentive",
            "실적": r"earnings|results|quarter|backlog|guidance|revenue",
        },
    },
    "Electricity": {
        "require_topic": True,
        "core": r"electricity|power grid|power (price|prices|demand|market)|wholesale power|blackout|power outage|transmission (line|grid|investment)|capacity market|peak demand",
        "topics": {
            "수급/가격": r"price|prices|wholesale|peak|demand|outage|blackout|shortage|record|spot",
            "정책/규제": r"tariff|rate case|rate hike|regulat|grid plan|transmission|FERC|policy|approve",
            "지정학": r"sanction|security|supply (disruption|cut)|gas|LNG|embargo|geopolit",
            "인프라": r"power plant|new plant|investment|interconnect|substation|build",
        },
    },
    "Module": {
        "require_topic": False,
        "core": r"(solar|PV) modules?|module (price|prices|maker|makers|manufactur|shipment)|Jinko|LONGi|Trina|JA Solar|Canadian Solar",
        "topics": {
            "가격/공급": r"price|prices|capacity|supply|shipment|inventory",
            "무역/정책": r"tariff|duties|origin|anti-?dumping|customs|\bFEOC\b|trade",
            "기업": r"order|expansion|restructur|layoff|loss|bankrupt|plant|factory|earnings|results",
        },
    },
    "Polysilicon": {
        "require_topic": False,
        "core": r"polysilicon|Tongwei|GCL Tech|Daqo|Wacker|OCI|Hemlock|silicon metal",
        "topics": {
            "가격/생산": r"price|prices|output|production|inventory|stockpile|utili[sz]ation",
            "정책/무역": r"tariff|export|Section 232|investigation|anti-?dumping|control|duties",
            "기업동향": r"expansion|cut|curtail|shutdown|bankrupt|consolidat|acquir|merger|plant",
        },
    },
}

COMPILED = {
    name: {
        "require_topic": cfg["require_topic"],
        "core": re.compile(cfg["core"], re.I),
        "topics": {t: re.compile(p, re.I) for t, p in cfg["topics"].items()},
    }
    for name, cfg in CATEGORIES.items()
}


def classify(text: str):
    """[(카테고리, [하위주제,...]), ...] 반환. 매칭 없으면 빈 리스트."""
    hits = []
    for name, cfg in COMPILED.items():
        if not cfg["core"].search(text):
            continue
        topics = [t for t, rx in cfg["topics"].items() if rx.search(text)]
        if cfg["require_topic"] and not topics:
            continue
        hits.append((name, topics))
    return hits


# ---------------------------------------------------------------------------
# 3) 수집 / 중복 제거
# ---------------------------------------------------------------------------
def strip_html(s: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", s or "")).strip()


def entry_time(e):
    for key in ("published_parsed", "updated_parsed"):
        t = getattr(e, key, None) or e.get(key)
        if t:
            return datetime(*t[:6], tzinfo=timezone.utc)
    return None


def fetch_feed(name: str, url: str):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return feedparser.parse(r.content).entries
    except Exception as ex:
        print(f"[WARN] {name} 수집 실패: {ex}")
        return []


def title_key(title: str) -> str:
    norm = re.sub(r"[^a-z0-9]+", "", title.lower())
    return hashlib.sha1(norm.encode()).hexdigest()[:16]


def clean_title(title: str, source: str) -> str:
    # Google News는 제목 끝에 ' - 매체명'을 붙임
    return re.sub(r"\s+-\s+[^-]{2,40}$", "", title).strip()


def load_seen():
    try:
        with open(SEEN_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_seen(seen: dict):
    cutoff = time.time() - 14 * 86400
    seen = {k: v for k, v in seen.items() if v > cutoff}
    with open(SEEN_FILE, "w") as f:
        json.dump(seen, f)


def collect():
    feeds = {n: google_news_url(d) for n, d in SOURCES.items()}
    feeds.update(DIRECT_FEEDS)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    items = []
    for name, url in feeds.items():
        for e in fetch_feed(name, url):
            ts = entry_time(e)
            if ts and ts < cutoff:
                continue
            title = clean_title(e.get("title", ""), name)
            if not title:
                continue
            summary = strip_html(e.get("summary", ""))
            hits = classify(f"{title} {summary}")
            if not hits:
                continue
            items.append({
                "source": name, "title": title, "link": e.get("link", ""),
                "time": ts or datetime.now(timezone.utc), "hits": hits,
            })
    items.sort(key=lambda x: x["time"])
    return items


# ---------------------------------------------------------------------------
# 4) 번역(선택) / 텔레그램 전송
# ---------------------------------------------------------------------------
def ko_title(title: str) -> str:
    if not ANTHROPIC_KEY:
        return ""
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 200,
                  "messages": [{"role": "user",
                                "content": f"다음 영어 뉴스 제목을 자연스러운 한국어 한 줄로 번역해. 번역문만 출력.\n{title}"}]},
            timeout=30,
        )
        return r.json()["content"][0]["text"].strip()
    except Exception as ex:
        print(f"[WARN] 번역 실패: {ex}")
        return ""


def build_message(it) -> str:
    tags = []
    for cat, topics in it["hits"]:
        tags.append("#" + re.sub(r"[^0-9A-Za-z가-힣]", "", cat))
        tags += ["#" + re.sub(r"[^0-9A-Za-z가-힣]", "", t) for t in topics[:2]]
    ko = ko_title(it["title"])
    lines = [f"<b>{html.escape(it['title'])}</b>"]
    if ko:
        lines.append(html.escape(ko))
    lines.append(f"📰 {html.escape(it['source'])}")
    lines.append(" ".join(dict.fromkeys(tags)))
    lines.append(f'<a href="{html.escape(it["link"])}">원문 보기</a>')
    return "\n".join(lines)


def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    for _ in range(3):
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code == 200:
            return True
        if r.status_code == 429:
            time.sleep(r.json().get("parameters", {}).get("retry_after", 10) + 1)
            continue
        print(f"[ERR] 텔레그램 {r.status_code}: {r.text}")
        return False
    return False


def main():
    if not BOT_TOKEN or not CHAT_ID:
        raise SystemExit("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 환경변수를 설정하세요.")
    seen = load_seen()
    sent = 0
    for it in collect():
        key = title_key(it["title"])
        if key in seen:
            continue
        if sent >= MAX_PER_RUN:
            break
        if send_telegram(build_message(it)):
            seen[key] = time.time()
            sent += 1
            time.sleep(SEND_INTERVAL)
    save_seen(seen)
    print(f"게시 {sent}건")


if __name__ == "__main__":
    main()
