// config.js
import fs from "fs";
import path from "path";
import dotenv from "dotenv";

const envPath = process.env.ENV_PATH && fs.existsSync(process.env.ENV_PATH)
  ? process.env.ENV_PATH
  : path.resolve(process.cwd(), ".env");

dotenv.config({ path: envPath });

const toInt = (v, d) => {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : d;
};

const toBool = (value, defaultValue) => {
  if (value === undefined) return defaultValue;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return defaultValue;
};

// === REST API endpoints ===
// /md/v2/Securities/:exchange/:symbol/alltrades/history
export function tradesHistoryPath({ baseUrl, exchange, symbol }) {
  return `${baseUrl}/md/v2/Securities/${encodeURIComponent(exchange)}/${encodeURIComponent(symbol)}/alltrades/history`;
}

// Query parameter builder (unified for Alor API)
export function buildTradesHistoryParams({ fromMs, toMs, limit }) {
  const toSecondsInt = (v) => {
    if (!Number.isFinite(v)) return undefined;
    // Convert milliseconds to seconds if value is clearly in ms range (>= 1e12)
    const seconds = v >= 1e12 ? Math.floor(v / 1000) : Math.floor(v);
    return String(seconds);
  };

  const params = {};
  // Send from/to in seconds (without milliseconds)
  const fromSec = toSecondsInt(fromMs);
  const toSec = toSecondsInt(toMs);
  if (fromSec !== undefined) params.from = fromSec;
  if (toSec !== undefined) params.to = toSec;
  if (Number.isFinite(limit)) params.limit = String(limit);

  return params;
}

export const CONFIG = Object.freeze({
  // токеен
  REFRESH_TOKEN: process.env.REFRESH_TOKEN,

  // API endpoints
  BASE_URL: process.env.BASE_URL,
  AUTH_URL: process.env.OAUTH_URL,
  WS_URL: process.env.WS_URL,

  // Trading instrument
  EXCHANGE: process.env.EXCHANGE,
  SYMBOL: process.env.SYMBOL,
  TICK_SIZE: Number(process.env.TICK_SIZE || 1),

  // API request settings
  PAGE_LIMIT: toInt(process.env.PAGE_LIMIT, 5000),

  // Working days parameters
  START_DATE: process.env.START_DATE, // Format: dd.MM.yyyy
  WORK_DAYS: toInt(process.env.WORK_DAYS, 5), // Number of working days (Mon-Fri)

  // для сохранения в файл
  FLUSH_EVERY_MS: Number(process.env.FLUSH_EVERY_MS || 10000),
  DATA_DIR: process.env.DATA_DIR || './data',
  MAX_FILE_SIZE_MB: Number(process.env.MAX_FILE_SIZE_MB || 100), // Максимальный размер файла в MB перед ротацией

  // глубина стакана
  DEPTH: Number(process.env.DEPTH || 30),

  // частота обновления стакана
  FREQUENCY: Number(process.env.FREQUENCY || 0),

  // Часовой пояс для торгов
  TIMEZONE: process.env.TIMEZONE || 'Europe/Moscow',

  // Расписание торгов (время в минутах от начала дня)
  TRADING_SESSIONS: [
    { start: 9 * 60, end: 14 * 60 },      // 9:00 - 14:00
    { start: 14 * 60 + 5, end: 18 * 60 + 50 },  // 14:05 - 18:50
    { start: 19 * 60 + 5, end: 23 * 60 + 50 }   // 19:05 - 23:50
  ],

  // Параметры агрегации сделок 
  // на таких разницах тиках агрегируются сделки
  TRADE_PRICE_ZONES: [1, 2, 4, 8, 16, Infinity],

  // интервалы по объему сделок
  TRADE_SIZE_BINS: [1, 2, 4, 12, 30, 60, 120, 300],

  // Тайм-константы (сек)
  TAU_EWMA_BESTVOL: 30, // масштаб для объёмов
  TAU_SWN_FAST: 15,     // SWN для быстрых фич (dVol/сек, trades/сек, micro speed)
  EPS: 1e-8,

  // Клипы
  CLIP_PRICE_OFFSET: 70,
  CLIP_SPREAD_TICKS: 15,
  CLIP_DVOL_RATE: 60,
  CLIP_Z_SCORE: 5,
  CLIP_TRADE_COUNT_PER_SEC: 80,
  CLIP_TRADE_VOL_PER_SEC: 520,

  // 1) Не включать "сырое" Δt как фичу (оставляем только logΔt)
  USE_RAW_DT: false,

  // Кап для Δt перед log (срезаем хвосты > p99)
  DT_MAX_SEC: 5.0,

  // 2) Использовать не абсолютный VWAP, а смещение в тиках: (vwap - mid)/tick
  USE_VWAP_OFFSET: true,
  
  // 3) Чтобы избежать константных колонок по хвостовым бинам — можно ограничить число бинов
  SIZE_BINS_KEEP: 6, // берем первые 6 бинов; ставь null, чтобы оставить все

  EWMA_ALPHA_BESTVOL: 0.025,

  // Расширенные параметры построения фичей
  DVOL_LEVELS: Number(process.env.DVOL_LEVELS || 5),
  GAP_LOOKUP_LEVELS: Number(process.env.GAP_LOOKUP_LEVELS || 10),
  GAP_VOLUME_THRESHOLD: Number(process.env.GAP_VOLUME_THRESHOLD || 0),
  GAP_LARGE_VOLUME_THRESHOLD: Number(process.env.GAP_LARGE_VOLUME_THRESHOLD || 0),
  TRADE_SIZE_HISTORY_MAX: toInt(process.env.TRADE_SIZE_HISTORY_MAX, 5000),
  TRADE_QUANTILE_BINS: toInt(process.env.TRADE_QUANTILE_BINS, 5),
  // выключить сохранение имен колонок фичей (ускоряет и уменьшает память)
  ENABLE_FEATURE_NAMES: toBool(process.env.ENABLE_FEATURE_NAMES, true),
});

// Get volume binning settings for specific symbol
// Sources:
// 1) ENV VOLUME_RULES_JSON — JSON like { "SiU5": { "qtyInterval": 10, "intervalCount": 10 } }
// 2) ENV defaults: QTY_INTERVAL, INTERVAL_COUNT (for all symbols)
export function getVolumeBinningForSymbol(symbol) {
  let qtyInterval = toInt(process.env.QTY_INTERVAL, 0);
  let intervalCount = toInt(process.env.INTERVAL_COUNT, 10);

  try {
    if (process.env.VOLUME_RULES_JSON) {
      const obj = JSON.parse(process.env.VOLUME_RULES_JSON);
      if (obj && obj[symbol]) {
        const r = obj[symbol];
        const q = toInt(r.qtyInterval, qtyInterval);
        const c = toInt(r.intervalCount, intervalCount);
        qtyInterval = q;
        intervalCount = c;
      }
    }
  } catch {}

  return { qtyInterval, intervalCount };
}
