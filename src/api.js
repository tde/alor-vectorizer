// api.js
import { http } from "./http.js";
import { CONFIG, tradesHistoryPath, buildTradesHistoryParams } from "./config.js";
import { getAccessToken } from "./auth.js";

// Sleep utility function
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

// Normalize trade data format
function normalizeTrade(raw) {
  return {
    ts: Number(raw.timestamp),
    qty: Number(raw.qty),
    price: Number(raw.price),
  };
}

// MSK timezone helpers (UTC+3)
function toMskDate(tsMs) {
  const MSK_OFFSET_MS = 3 * 60 * 60 * 1000;
  return new Date(tsMs + MSK_OFFSET_MS);
}

function formatMsk(tsMs) {
  const d = toMskDate(tsMs);
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const MM = String(d.getUTCMonth() + 1).padStart(2, '0');
  const yyyy = String(d.getUTCFullYear());
  const HH = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  return `${dd}.${MM}.${yyyy} ${HH}:${mm}`;
}

// Filter trades to only allow 09:00–23:58 MSK
function isAllowedMskMinute(tsMs) {
  if (!Number.isFinite(tsMs)) return false;
  const MSK_OFFSET_MS = 3 * 60 * 60 * 1000;
  const msk = new Date(tsMs + MSK_OFFSET_MS);
  const h = msk.getUTCHours();
  const m = msk.getUTCMinutes();
  
  if (h < 9) return false;            // before 09:00 MSK — exclude
  if (h > 23) return false;           // safety check (won't happen)
  if (h < 23) return true;            // 09:00..22:59 — allow
  return m <= 58;                      // 23:00..23:58 — allow, 23:59 — exclude
}

// Skip HTTP requests for hours between 00:00 and 08:59 MSK
function shouldSkipHourMsk(tsMsHourStart) {
  const MSK_OFFSET_MS = 3 * 60 * 60 * 1000;
  const msk = new Date(tsMsHourStart + MSK_OFFSET_MS);
  const h = msk.getUTCHours();
  return h < 9; // 00..08 — skip
}

// Date parsing helper
function parseDdMmYyyy(dateStr) {
  if (!dateStr) return null;
  const m = /^([0-3]?\d)\.([01]?\d)\.(\d{4})$/.exec(String(dateStr).trim());
  if (!m) return null;
  
  const dd = Number(m[1]);
  const MM = Number(m[2]);
  const yyyy = Number(m[3]);
  const d = new Date(yyyy, MM - 1, dd, 0, 0, 0, 0); // local midnight
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

// Weekend check
function isWeekend(dateObj) {
  const w = dateObj.getDay();
  return w === 0 || w === 6; // Sunday=0, Saturday=6
}

// Fetch trades by working days, starting from date (dd.MM.yyyy), not exceeding current date
export async function fetchTradesHourlyByWorkingDays(startDateStr, workDays) {
  const startDate = parseDdMmYyyy(startDateStr);
  if (!startDate) throw new Error("Invalid CONFIG.START_DATE (expected dd.MM.yyyy)");

  const nowMs = Date.now();
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const all = [];
  let collectedDays = 0;
  let overallLogged = false;
  let overallStartMs = null;

  for (let d = new Date(startDate); collectedDays < workDays && d.getTime() <= today.getTime(); d.setDate(d.getDate() + 1)) {
    if (isWeekend(d)) {
      continue;
    }
    collectedDays += 1;

    const dayStart = new Date(d.getFullYear(), d.getMonth(), d.getDate(), 0, 0, 0, 0).getTime();
    const dayEnd = Math.min(dayStart + 24 * 60 * 60 * 1000 - 1, nowMs);

    if (!overallLogged) {
      overallStartMs = dayStart;
      console.log(`[i] Период запросов (рабочие дни): ${formatMsk(overallStartMs)} — ${formatMsk(dayEnd)}`);
      overallLogged = true;
    }

    for (let tStart = dayStart; tStart <= dayEnd; tStart += 60 * 60 * 1000) {
      if (shouldSkipHourMsk(tStart)) {
        continue;
      }
      
      const tEnd = Math.min(tStart + 60 * 60 * 1000 - 1, dayEnd);

      console.log(`[i] День: ${formatMsk(tStart)} — ${formatMsk(tEnd)}`);

      const access = await getAccessToken();
      const url = tradesHistoryPath({
        baseUrl: CONFIG.BASE_URL,
        exchange: CONFIG.EXCHANGE,
        symbol: CONFIG.SYMBOL
      });
      
      const params = buildTradesHistoryParams({ fromMs: tStart, toMs: tEnd, limit: CONFIG.PAGE_LIMIT });

      const rsp = await http.get(url, {
        headers: { 'Authorization': `Bearer ${access}`, 'Accept': 'application/json' },
        params,
        validateStatus: () => true,
      });

      if (rsp.status < 200 || rsp.status >= 300) {
        throw new Error(`HTTP ${rsp.status}: ${JSON.stringify(rsp.data)}`);
      }

      const data = rsp.data;
      const items = Array.isArray(data?.list) ? data.list : Array.isArray(data) ? data : [];
      
      for (const r of items) {
        const t = normalizeTrade(r);
        if (Number.isFinite(t.ts) && t.ts >= tStart && t.ts <= tEnd && isAllowedMskMinute(t.ts)) {
          all.push(t);
        }
      }

      if (rsp.data.total >= CONFIG.PAGE_LIMIT - 1) {
        throw new Error(`Кол-во записей получены не все: ${rsp.data.total}`);
      } else {
        console.log(`получено кол-во сделок= ${rsp.data.total}`);
      }
    }
  }

  return all;
}