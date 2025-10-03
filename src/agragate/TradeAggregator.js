import { msToSec } from "../utils.js";
import { CONFIG } from "../config.js";

export class TradeAggregator {
    constructor() {
        this.trades = []; // хранит последние N секунд, можно чистить по времени
        this.sizeHistory = [];
        this.lastTradePrice = null;
        this.lastTradeSide = null;
        this.sizeHistoryLimit = CONFIG.TRADE_SIZE_HISTORY_MAX || 5000;
        this.prevTradePrice = null;
    }
  
    push(trade) {
        const t = {
          tsMs: trade.data?.timestamp ?? Date.parse(trade.data?.time),
          price: trade.data?.price,
          qty: trade.data?.qty ?? trade.data?.volume ?? 0,
          side: this._normalizeSide(trade.data?.side),
        };

        if (!Number.isFinite(t.tsMs) || !t.price || !t.qty) return;

        t.side = this._inferSide(t.side, t.price);
        this._registerTradeMeta(t.price, t.side);

        this.trades.push(t);
        this._updateSizeHistory(t.qty);
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

        const quantileBins = this._initQuantileBins();
        if (quantileBins) {
            const { counts, volumes, thresholds } = quantileBins;
            for (const tr of this.trades) {
              if (tr.tsMs <= prevMs || tr.tsMs > currMs) continue;
              const binIdx = this._quantileBinIndex(tr.qty, thresholds);
              counts[binIdx] += 1;
              volumes[binIdx] += tr.qty;
            }
            // превратим в скорость, как и остальные метрики
            for (let i = 0; i < counts.length; i++) {
              counts[i] = counts[i] / Δt;
              volumes[i] = volumes[i] / Δt;
            }
            quantileBins.counts = counts;
            quantileBins.volumes = volumes;
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

        if (quantileBins) {
            res.quantileBinsCount = quantileBins.counts;
            res.quantileBinsVol = quantileBins.volumes;
        }
        
        // (опционально) урезаем хвостовые бины, чтобы не плодить константы
        if (Number.isFinite(CONFIG.SIZE_BINS_KEEP)) {
            res.sizeBinsCountPerSec = res.sizeBinsCountPerSec.slice(0, CONFIG.SIZE_BINS_KEEP);
            res.sizeBinsVolPerSec   = res.sizeBinsVolPerSec.slice(0, CONFIG.SIZE_BINS_KEEP);
        }

        return res;
    }

    _normalizeSide(side) {
        if (!side) return null;
        const s = String(side).toLowerCase();
        if (s === "buy" || s === "sell") return s;
        return null;
    }

    _inferSide(side, price) {
        if (side === "buy" || side === "sell") return side;
        if (!Number.isFinite(this.lastTradePrice)) return "unknown";
        if (price > this.lastTradePrice) return "buy";
        if (price < this.lastTradePrice) return "sell";
        return this.lastTradeSide || "unknown";
    }

    _registerTradeMeta(price, side) {
        if (Number.isFinite(price)) {
            this.lastTradePrice = price;
        }
        if (side === "buy" || side === "sell") {
            this.lastTradeSide = side;
        }
    }

    _updateSizeHistory(qty) {
        if (!Number.isFinite(qty) || qty <= 0) return;
        this.sizeHistory.push(qty);
        if (this.sizeHistory.length > this.sizeHistoryLimit) {
            this.sizeHistory.splice(0, this.sizeHistory.length - this.sizeHistoryLimit);
        }
    }

    _initQuantileBins() {
        const bins = CONFIG.TRADE_QUANTILE_BINS || 0;
        if (bins <= 0) return null;
        if (this.sizeHistory.length < bins) return null;
        const thresholds = this._quantileThresholds(bins - 1);
        return {
            counts: new Array(bins).fill(0),
            volumes: new Array(bins).fill(0),
            thresholds,
        };
    }

    _quantileThresholds(cuts) {
        if (cuts <= 0) return [];
        const sorted = [...this.sizeHistory].sort((a, b) => a - b);
        const thresholds = [];
        for (let i = 1; i <= cuts; i++) {
            const pos = (i * (sorted.length + 1)) / (cuts + 1);
            const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(pos) - 1));
            thresholds.push(sorted[idx]);
        }
        return thresholds;
    }

    _quantileBinIndex(qty, thresholds) {
        for (let i = 0; i < thresholds.length; i++) {
            if (qty <= thresholds[i]) return i;
        }
        return thresholds.length;
    }
}
