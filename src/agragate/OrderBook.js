import { CONFIG } from "../config.js";
import { safeDiv, clip, log1p } from "../utils.js";

export class OrderBook {
    constructor() {
        this.prevMaps = null; // {bid: Map, ask: Map}
        this.prevMs = null;
        this.prevMicro = null;
        this.ewmaBestVol = null; // EWMA масштаба объёмов
        this.prevSpreadTicks = null;
        this.prevCogBid = null;
        this.prevCogAsk = null;
    }

    _micro(bid, ask, bidV, askV) {
        const denom = (bidV + askV) || 1e-6;
        return (ask * bidV + bid * askV) / denom;
    }

    _normalizeSide(levels, depth) {
        const lastPrice = levels[levels.length - 1].price;
        while (levels.length < depth) {
            levels.push({price: lastPrice, volume: 0});
        }
    }

    applySnapshot(msg) {
        const depth = CONFIG.DEPTH;
        const d = msg.data || {};
        const bids = (d.bids || []).slice(0, CONFIG.LEVELS_PER_SIDE);
        const asks = (d.asks || []).slice(0, CONFIG.LEVELS_PER_SIDE);
        if (!bids.length || !asks.length) return null;

        // если не все уровни в стакане, то дополнить
        if (bids.length < depth) {
            this._normalizeSide(bids, depth);
        }
        if (asks.length < depth) {
            this._normalizeSide(asks, depth);
        }

        const ms = d.ms_timestamp || (d.timestamp * 1000) || 0;
        const bestBid = bids[0].price;
        const bestAsk = asks[0].price;
        const mid = (bestBid + bestAsk) / 2;
        const spreadTicks = clip(safeDiv(bestAsk - bestBid, CONFIG.TICK_SIZE, 0), -CONFIG.CLIP_SPREAD_TICKS, CONFIG.CLIP_SPREAD_TICKS);

        // EWMA масштаба объёмов (best bid+ask)
        const bestVol = (bids[0].volume || 0) + (asks[0].volume || 0);
        this.ewmaBestVol = this._ewmaUpdate(this.ewmaBestVol, bestVol, ms, CONFIG.TAU_EWMA_BESTVOL) || Math.max(bestVol, 1);

        const priceOffset = (p) => clip(
            (p - mid) / CONFIG.TICK_SIZE,
            -CONFIG.CLIP_PRICE_OFFSET, CONFIG.CLIP_PRICE_OFFSET
        );
        const normVol = (v) => log1p(v / Math.max(this.ewmaBestVol, 1e-6));

        const mapBidPrev = this.prevMaps?.bid ?? new Map();
        const mapAskPrev = this.prevMaps?.ask ?? new Map();
        const mapBid = new Map(); const mapAsk = new Map();

        const toSide = (levels, prevMap) => {
            return levels.map(({ price, volume }) => ({
                price,
                po: priceOffset(price),
                lv: normVol(volume),
                v: volume,
                prevV: prevMap.get(price) ?? null,
            }));
        };

        const sideBid = toSide(bids, mapBidPrev);
        const sideAsk = toSide(asks, mapAskPrev);
        for (const x of sideBid) mapBid.set(x.price, x.v);
        for (const x of sideAsk) mapAsk.set(x.price, x.v);

        const micro = this._micro(bestBid, bestAsk, bids[0]?.volume || 0, asks[0]?.volume || 0);
        const ΔtSec = this.prevMs ? (ms - this.prevMs) / 1000 : 0;
        const microSpeed = (this.prevMicro != null && ΔtSec > 0) ? (micro - this.prevMicro) / ΔtSec : 0;

        const dvolLevels = Math.max(1, CONFIG.DVOL_LEVELS || 3);
        const buildDvol = (arr) => {
            const out = new Array(Math.min(dvolLevels, arr.length)).fill(0);
            if (!(this.prevMs && ΔtSec > 0)) return out;
            for (let i = 0; i < out.length; i++) {
                const level = arr[i];
                if (!level) {
                    out[i] = 0;
                    continue;
                }
                if (level.prevV == null) {
                    out[i] = 0;
                    continue;
                }
                const rate = (level.v - level.prevV) / ΔtSec;
                out[i] = clip(rate, -CONFIG.CLIP_DVOL_RATE, CONFIG.CLIP_DVOL_RATE);
            }
            return out;
        };

        const cog = (arr) => {
            let sumVol = 0;
            let weighted = 0;
            for (let i = 0; i < arr.length; i++) {
                const vol = arr[i]?.v ?? 0;
                sumVol += vol;
                weighted += vol * (i + 1);
            }
            if (sumVol <= 0) return 0;
            return weighted / sumVol;
        };

        const cogBid = cog(sideBid);
        const cogAsk = cog(sideAsk);
        const cogBidSpeed = (this.prevCogBid != null && ΔtSec > 0) ? (cogBid - this.prevCogBid) / ΔtSec : 0;
        const cogAskSpeed = (this.prevCogAsk != null && ΔtSec > 0) ? (cogAsk - this.prevCogAsk) / ΔtSec : 0;

        const spreadPerSec = (this.prevSpreadTicks != null && ΔtSec > 0)
            ? (spreadTicks - this.prevSpreadTicks) / ΔtSec
            : 0;

        const out = {
            ms, mid, spreadTicks,
            ΔtSec, micro, microSpeed,
            bid: sideBid, ask: sideAsk,
            dvolBidTop: buildDvol(sideBid),
            dvolAskTop: buildDvol(sideAsk),
            spreadPerSec,
            cogBid,
            cogAsk,
            cogBidSpeed,
            cogAskSpeed,
        };

        this.prevMaps = { bid: mapBid, ask: mapAsk };
        this.prevMs = ms;
        this.prevMicro = micro;
        this.prevSpreadTicks = spreadTicks;
        this.prevCogBid = cogBid;
        this.prevCogAsk = cogAsk;
        return out;
    }

    _ewmaUpdate(prev, x, tsMs, tauSec) {
        if (prev == null || this.prevMs == null) return x;
        const dtSec = (tsMs - this.prevMs) / 1000;
        const a = 1 - Math.exp(-dtSec / tauSec);
        return a * x + (1 - a) * prev;
    }
}
