// index.js - Trades statistics module
import { CONFIG, getVolumeBinningForSymbol } from "./config.js";
import { fetchTradesHourlyByWorkingDays } from "./api.js";

export async function runTradesStats() {
  console.log(`[i] Инструмент: ${CONFIG.EXCHANGE}:${CONFIG.SYMBOL}`);
  
  const trades = await fetchTradesHourlyByWorkingDays(CONFIG.START_DATE, CONFIG.WORK_DAYS);
  
  console.log(`[i] Сделок получено: ${trades.length}`);

  if (trades.length === 0) {
    console.log("[i] Пустой результат, нечего выводить");
    return;
  }

  // Volume binning from config (explicit intervals)
  const { qtyInterval, intervalCount } = getVolumeBinningForSymbol(CONFIG.SYMBOL);
  if (!Number.isFinite(qtyInterval) || qtyInterval <= 0) {
    throw new Error("Некорректный qtyInterval (ожидается положительное число)");
  }
  
  const parts = Number.isFinite(intervalCount) && intervalCount > 0 ? intervalCount : 10;

  // Minimum lot size is always 1
  const minQty = 1;

  const buckets = Array.from({ length: parts + 1 }, () => 0);
  const maxQtyBucket = qtyInterval * intervalCount;
  
  for (const t of trades) {
    let idx = 0;
    if (t.qty > maxQtyBucket) {
      idx = parts;
    } else {
      idx = Math.min(parts - 1, Math.max(0, Math.floor((t.qty - minQty) / qtyInterval)));
    }
    buckets[idx] += 1;
  }

  console.log(`[i] Отрезки по объёму (${parts} шт., шаг=${qtyInterval}) и количество сделок:`);
  for (let i = 0; i < parts + 1; i++) {
    const start = minQty + i * qtyInterval;
    const end = i >= parts ? 99999 : start + qtyInterval - (i === parts - 1 ? 0 : 1);
    console.log(`[${start} .. ${end}] : ${buckets[i]}`);
  }
}
