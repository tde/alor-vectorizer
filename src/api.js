// api.js
import { http } from "./http.js";
import { CONFIG, tradesHistoryPath, buildTradesHistoryParams } from "./config.js";
import { getAccessToken } from "./auth.js";
import { formatMsk, isAllowedMskMinute, shouldSkipHourMsk, parseDdMmYyyy, isWeekend } from "./utils.js";
import { DateTime } from 'luxon';

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


// Fetch trades by working days, starting from date (dd.MM.yyyy), not exceeding current date
export async function fetchTradesHourlyByWorkingDays(startDateStr, workDays) {
  const startDate = parseDdMmYyyy(startDateStr);
  if (!startDate) throw new Error("Invalid CONFIG.START_DATE (expected dd.MM.yyyy)");

  const nowMs = DateTime.now().toMillis();
  const today = DateTime.now().startOf('day');

  const all = [];
  let collectedDays = 0;
  let overallLogged = false;
  let overallStartMs = null;

  for (let d = startDate; collectedDays < workDays && d.toMillis() <= today.toMillis(); d = d.plus({ days: 1 })) {
    if (isWeekend(d)) {
      continue;
    }
    collectedDays += 1;

    const dayStart = d.startOf('day').toMillis();
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