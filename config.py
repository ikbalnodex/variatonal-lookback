"""
Configuration for Lookback Bot - BTC/ETH Price History Collector
"""
import os
import logging

# =============================================================================
# Telegram (untuk heartbeat saja)
# =============================================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# =============================================================================
# Upstash Redis — pakai kredensial yang SAMA dengan bot utama
# =============================================================================
UPSTASH_REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "")
UPSTASH_REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

# =============================================================================
# API Configuration
# =============================================================================
API_BASE_URL = "https://omni-client-api.prod.ap-northeast-1.variational.io"
API_ENDPOINT = "/metadata/stats"

# =============================================================================
# Timing
# =============================================================================
SCAN_INTERVAL_SECONDS = 180       # 3 menit — sama dengan bot utama
HEARTBEAT_MINUTES = 30            # Laporan rutin ke Telegram
FRESHNESS_THRESHOLD_MINUTES = 10
MAX_LOOKBACK_HOURS = 24           # Simpan maksimal 24 jam history

# =============================================================================
# Logging
# =============================================================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("lookback_bot")
