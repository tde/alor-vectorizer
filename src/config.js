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

  // Расписание торгов (время в минутах от начала дня)
  TRADING_SESSIONS: [
    { start: 9 * 60, end: 14 * 60 },      // 9:00 - 14:00
    { start: 14 * 60 + 5, end: 18 * 60 + 50 },  // 14:05 - 18:50
    { start: 19 * 60 + 5, end: 23 * 60 + 50 }   // 19:05 - 23:50
  ],
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
