import { CONFIG } from "../config.js";
import { safeDiv, ewmaUpdate, clip, log1p } from "../utils.js";

export class OrderBook {
    constructor() {
        this.prev = null; // предыдущий снимок (словарь по цене)
        this.curr = null; // текущий снимок
        this.bestVolEwma = null; // для нормировки объёмов
        this.prevMicro = null;
        this.prevMs = null;
    }
  
    // ожидаем структуру из примера (snapshot bids/asks, ms_timestamp)
    applySnapshot(snap) {
        const ms = snap.data?.ms_timestamp || (snap.data?.timestamp * 1000) || 0;
        const bids = (snap.data?.bids || []).slice(0, CONFIG.LEVELS_PER_SIDE);
        const asks = (snap.data?.asks || []).slice(0, CONFIG.LEVELS_PER_SIDE);
        const bestBid = bids[0].price;
        const bestAsk = asks[0].price;

        if (!bids.length || !asks.length) return null;

        const mid = (bestBid + bestAsk) / 2;
        const spreadTicks = safeDiv(bestAsk - bestBid, CONFIG.TICK_PRICE, 0);

        // обновим EWMA объёма на best (на обеих сторонах)
        const bestVol = (bids[0].volume || 0) + (asks[0].volume || 0);
        this.bestVolEwma = ewmaUpdate(this.bestVolEwma, bestVol, CONFIG.EWMA_ALPHA_BESTVOL) || 1;

    
        // словари объёмов по exact price для dvol
        const prevMapBid = this.curr?.mapBid ?? new Map();
        const prevMapAsk = this.curr?.mapAsk ?? new Map();
        const mapBid = new Map();
        const mapAsk = new Map();

        const priceOffset = (p) =>
            clip((p - mid) / CONFIG.TICK_PRICE, -CONFIG.CLIP_PRICE_OFFSET, CONFIG.CLIP_PRICE_OFFSET);
      
        const normVol = (v) =>
            clip(log1p(v / Math.max(this.bestVolEwma, 1e-6)), -CONFIG.CLIP_LOG_VOL, CONFIG.CLIP_LOG_VOL);
      

        return {ms, bids, asks, mid};
    }

    //вывод в простом формате цен и объемов
    // вывод заданное кол-во уровней в виде [цена, объем] в две строки первая по bid, вторая по ask
    getSimpleFormat(levelsPerSide, state) {
        const {bids, asks} = state; 
        const limitedBids = bids.slice(0, levelsPerSide);
        const limitedAsks = asks.slice(0, levelsPerSide);
        return `${limitedBids.map(bid => `[${bid.price}, ${bid.volume}]`).join(' ')}\n${limitedAsks.map(ask => `[${ask.price}, ${ask.volume}]`).join(' ')}`;
    }
}