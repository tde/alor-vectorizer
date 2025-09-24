import { msToSec } from "../utils.js";
import { CONFIG } from "../config.js";

export class TradeAggregator {
    constructor() {
        this.trades = []; // хранит последние N секунд, можно чистить по времени
    }
  
    push(trade) {
        const t = {
          tsMs: trade.data?.timestamp ?? Date.parse(trade.data?.time),
          price: trade.data?.price,
          qty: trade.data?.qty ?? trade.data?.volume ?? 0,
          side: trade.data?.side?.toLowerCase() || null, // "buy"/"sell" если есть
        };

        if (!Number.isFinite(t.tsMs) || !t.price || !t.qty) return;
        
        this.trades.push(t);
    }

    clear() {
        this.trades = [];
    }

    _zoneIdx(mid, tick, price) {
        const dist = Math.abs(price - mid) / tick;
        const zones = CONFIG.TRADE_PRICE_ZONES;
        for (let i = 0; i < zones.length; i++) if (dist <= zones[i]) return i;
        return zones.length - 1;
    }
     _sizeBin(qty) {
        const B = CONFIG.TRADE_SIZE_BINS;
        for (let i = 0; i < B.length; i++) if (qty <= B[i]) return i;
        return B.length;
    }

    // Возвращает агрегаты по окну (prevMs, currMs], учитывая mid и tick для разбиения по зонам
    getWindowAggregates(prevMs, currMs, mid, tick) {
        const Δt = msToSec(currMs - prevMs) || 1e-6;
        const Z = CONFIG.TRADE_PRICE_ZONES.length;
        const perZone = Array.from({ length: Z }, () => ({
          bid: { c: 0, v: 0, vnum: 0 },
          ask: { c: 0, v: 0, vnum: 0 },
        }));
        const sizeCnt = Array(CONFIG.TRADE_SIZE_BINS.length + 1).fill(0);
        const sizeVol = Array(CONFIG.TRADE_SIZE_BINS.length + 1).fill(0);

        for (const tr of this.trades) {
            if (tr.tsMs <= prevMs || tr.tsMs > currMs) { /*i++; */ continue; }
            const side = tr.side || "unknown";
            const zi = this._zoneIdx(mid, tick, tr.price);
            const bi = this._sizeBin(tr.qty);
      
            sizeCnt[bi] += 1;
            sizeVol[bi] += tr.qty;
      
            if (side !== "unknown") {
              const key = side === "buy" ? "bid" : "ask"; // buy бьёт ask → сила bid
              perZone[zi][key].c += 1;
              perZone[zi][key].v += tr.qty;
              perZone[zi][key].vnum += tr.price * tr.qty;
            }
            this.prevTradePrice = tr.price;
        }

        const zones = perZone.map((o) => {
            const bid_v = o.bid.v, ask_v = o.ask.v;
            const bid_vwap_abs = bid_v > 0 ? o.bid.vnum / bid_v : 0;
            const ask_vwap_abs = ask_v > 0 ? o.ask.vnum / ask_v : 0;
            // VWAP в тиках относительно текущего mid (если включено)
            const bid_vwap_off = CONFIG.USE_VWAP_OFFSET ? (bid_vwap_abs - mid) / tick : bid_vwap_abs;
            const ask_vwap_off = CONFIG.USE_VWAP_OFFSET ? (ask_vwap_abs - mid) / tick : ask_vwap_abs;
            return {
                bid_count_per_sec: o.bid.c / Δt,
                bid_vol_per_sec:   o.bid.v / Δt,
                bid_vwap_off,
                ask_count_per_sec: o.ask.c / Δt,
                ask_vol_per_sec:   o.ask.v / Δt,
                ask_vwap_off,
            };
        });
      
        let res = {
            ΔtSec: Δt,
            zones, 
            sizeBinsCountPerSec: sizeCnt.map((x) => x / Δt),
            sizeBinsVolPerSec:   sizeVol.map((x) => x / Δt),
        };
        
        // (опционально) урезаем хвостовые бины, чтобы не плодить константы
        if (Number.isFinite(CONFIG.SIZE_BINS_KEEP)) {
            res.sizeBinsCountPerSec = res.sizeBinsCountPerSec.slice(0, CONFIG.SIZE_BINS_KEEP);
            res.sizeBinsVolPerSec   = res.sizeBinsVolPerSec.slice(0, CONFIG.SIZE_BINS_KEEP);
        }

        return res;
    }
}