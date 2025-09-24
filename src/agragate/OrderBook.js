import { CONFIG } from "../config.js";
import { safeDiv, clip, log1p } from "../utils.js";

export class OrderBook {
  constructor() {
    this.prevMaps = null; // {bid: Map, ask: Map}
    this.prevMs = null;
    this.prevMicro = null;
    this.ewmaBestVol = null; // EWMA масштаба объёмов
  }

  _micro(bid, ask, bidV, askV) {
    const denom = (bidV + askV) || 1e-6;
    return (ask * bidV + bid * askV) / denom;
  }

  applySnapshot(msg) {
    const d = msg.data || {};
    const bids = (d.bids || []).slice(0, CONFIG.LEVELS_PER_SIDE);
    const asks = (d.asks || []).slice(0, CONFIG.LEVELS_PER_SIDE);
    if (!bids.length || !asks.length) return null;

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

    const dvolSide = (arr) => {
      if (!(this.prevMs && ΔtSec > 0)) return [0,0,0];
      return arr.slice(0, 3).map((x) => {
        if (x.prevV == null) return 0;
        const rate = (x.v - x.prevV) / ΔtSec;
        return clip(rate, -CONFIG.CLIP_DVOL_RATE, CONFIG.CLIP_DVOL_RATE);
      });
    };

    const out = {
      ms, mid, spreadTicks,
      ΔtSec, micro, microSpeed,
      bid: sideBid, ask: sideAsk,
      dvolBidTop3: dvolSide(sideBid),
      dvolAskTop3: dvolSide(sideAsk),
    };

    this.prevMaps = { bid: mapBid, ask: mapAsk };
    this.prevMs = ms;
    this.prevMicro = micro;
    return out;
  }

  _ewmaUpdate(prev, x, tsMs, tauSec) {
    if (prev == null || this.prevMs == null) return x;
    const dtSec = (tsMs - this.prevMs) / 1000;
    const a = 1 - Math.exp(-dtSec / tauSec);
    return a * x + (1 - a) * prev;
  }
}
