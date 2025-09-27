import { CONFIG } from "../config.js";
import { clip } from "../utils.js";

/**
 * Собираем фичи для одного снимка с учётом сделок между снимками.
 * Применяем SWN/EWMA+клипы там, где нужно.
 */
export function buildFeatures(ob, aggs, normalizer) {
    if (!ob || !aggs) return null;

    const { ms, mid, spreadTicks, micro, microSpeed, ΔtSec, bid, ask, dvolBidTop3, dvolAskTop3 } = ob;
 
    // 1) глобальные скаляры
    const microDelta = clip(micro - mid, -CONFIG.CLIP_PRICE_OFFSET, CONFIG.CLIP_PRICE_OFFSET);
    const zMicroDelta = normalizer.zMicroDelta(ms, microDelta);
    const zMicroSpeed = normalizer.zMicroSpeed(ms, microSpeed);

    // капаем Δt и используем только logΔt (если так настроено)
    const dtCapped = Math.min(ΔtSec, CONFIG.DT_MAX_SEC);
    const scalars = [
        ...(CONFIG.USE_RAW_DT ? [dtCapped] : []),
        Math.log(Math.max(dtCapped, 1e-8)),
        clip(spreadTicks, -CONFIG.CLIP_SPREAD_TICKS, CONFIG.CLIP_SPREAD_TICKS),
        zMicroDelta, zMicroSpeed,
    ];

    // 2) уровни — price_offset и log_vol (уже нормированы EWMA(bestVol))
    const priceOffsetsBid = bid.map(x => x.po);
    const priceOffsetsAsk = ask.map(x => x.po);
    const logVolBid = bid.map(x => x.lv);
    const logVolAsk = ask.map(x => x.lv);

    // 3) dvol/сек top3 (оба направления) → SWN z + мягкий клип на входе уже есть
    const dvolTop3 = normalizer.zArray(ms, [...dvolBidTop3, ...dvolAskTop3], "dvol");

    // 4) сделки по зонам
    const zoneFeats = [];
    for (const z of aggs.zones) {
        // клипы по p99 → z-score
        const bc = normalizer.zTrCnt(ms, normalizer.clipTradeCnt(z.bid_count_per_sec));
        const bv = normalizer.zTrVol(ms, normalizer.clipTradeVol(z.bid_vol_per_sec));
        const ac = normalizer.zTrCnt(ms, normalizer.clipTradeCnt(z.ask_count_per_sec));
        const av = normalizer.zTrVol(ms, normalizer.clipTradeVol(z.ask_vol_per_sec));
        // VWAP-offset в тиках (уже из агрегатора) → z-score
        const bvo = normalizer.zVwapOff(ms, z.bid_vwap_off || 0);
        const avo = normalizer.zVwapOff(ms, z.ask_vwap_off || 0);
        zoneFeats.push(bc, bv, bvo, ac, av, avo);
    }

    // 5) бины размеров сделок (count/сек, vol/сек)
    const sizeCnt = normalizer.zArray(
        ms,
        aggs.sizeBinsCountPerSec.map(normalizer.clipTradeCnt.bind(normalizer)),
        "trCnt"
    );
    const sizeVol = normalizer.zArray(
        ms,
        aggs.sizeBinsVolPerSec.map(normalizer.clipTradeVol.bind(normalizer)),
        "trVol"
    );

    const vector = [
        ...scalars,
        ...priceOffsetsBid, ...priceOffsetsAsk,
        ...logVolBid, ...logVolAsk,
        ...dvolTop3,
        ...zoneFeats,
        ...sizeCnt, ...sizeVol,
    ];

    return { vector: Float32Array.from(vector) };
}
