import { CONFIG } from "../config.js";
import { clip, log1p } from "../utils.js";
import { FeatureCollector } from "./FeatureCollector.js";

const MIN_DT = 1e-8;
const IMBALANCE_K = [1, 3, 5, 10, 20];
const DEPTH_K = IMBALANCE_K;
const DEPTH_RATIO_K = IMBALANCE_K;
const DVOL_SUM_K = [1, 3, 5];
const GAP_ZERO_WINDOWS = [5, 10];

/**
 * Собираем фичи для одного снимка с учётом сделок между снимками.
 */
export function buildFeatures(ob, aggs, normalizer) {
    if (!ob || !aggs) return null;

    const {
        ms,
        mid,
        micro,
        microSpeed,
        ΔtSec,
        spreadTicks,
        spreadPerSec = 0,
        bid,
        ask,
        dvolBidTop = [],
        dvolAskTop = [],
        cogBid = 0,
        cogAsk = 0,
        cogBidSpeed = 0,
        cogAskSpeed = 0,
    } = ob;

    const collector = new FeatureCollector();

    const bookBid = Array.isArray(bid) ? bid : [];
    const bookAsk = Array.isArray(ask) ? ask : [];

    addScalarDynamics({ ms, mid, micro, microSpeed, ΔtSec, spreadTicks, spreadPerSec }, normalizer, collector);
    addOrderBookLevels(bookBid, bookAsk, collector);
    addDepthAndImbalance(ms, bookBid, bookAsk, normalizer, collector);
    addShapeMetrics(bookBid, bookAsk, normalizer, collector);
    addVolumeDynamics({ ms, dvolBidTop, dvolAskTop, cogBid, cogAsk, cogBidSpeed, cogAskSpeed }, normalizer, collector);
    addGapSignals(bookBid, bookAsk, collector);
    addTradeZones(ms, aggs.zones, normalizer, collector);
    addTradeSizeBins(ms, aggs, normalizer, collector);
    addTradeQuantiles(ms, aggs, normalizer, collector);

    return {
        vector: collector.toFloat32(),
        featureNames: collector.names(),
    };
}

function addScalarDynamics({ ms, mid, micro, microSpeed, ΔtSec, spreadTicks, spreadPerSec }, normalizer, collector) {
    const dtCapped = Math.min(ΔtSec, CONFIG.DT_MAX_SEC);
    const dtValue = Math.max(dtCapped, MIN_DT);
    const microDelta = clip(micro - mid, -CONFIG.CLIP_PRICE_OFFSET, CONFIG.CLIP_PRICE_OFFSET);
    const zMicroDelta = normalizer.zMicroDelta(ms, microDelta);
    const zMicroSpeed = normalizer.zMicroSpeed(ms, microSpeed);
    const spreadFeature = clip(spreadTicks, -CONFIG.CLIP_SPREAD_TICKS, CONFIG.CLIP_SPREAD_TICKS);
    const spreadPerSecClipped = clip(spreadPerSec ?? 0, -CONFIG.CLIP_DVOL_RATE, CONFIG.CLIP_DVOL_RATE);
    const zSpreadPerSec = normalizer.zSpreadSpeed(ms, spreadPerSecClipped);

    if (CONFIG.USE_RAW_DT) collector.add("scalar_dt_sec", dtCapped);
    collector.add("scalar_log_dt", Math.log(dtValue));
    collector.add("scalar_spread_ticks", spreadFeature);
    collector.add("scalar_spread_per_sec", spreadPerSecClipped);
    collector.add("scalar_spread_per_sec_z", zSpreadPerSec);
    collector.add("scalar_microprice", micro);
    collector.add("scalar_micro_minus_mid", microDelta);
    collector.add("scalar_micro_delta_z", zMicroDelta);
    collector.add("scalar_micro_speed", microSpeed);
    collector.add("scalar_micro_speed_z", zMicroSpeed);
}

function addOrderBookLevels(bid, ask, collector) {
    const priceOffsetsBid = bid.map((x) => x.po);
    const priceOffsetsAsk = ask.map((x) => x.po);
    const logVolBid = bid.map((x) => x.lv);
    const logVolAsk = ask.map((x) => x.lv);
    const depth = CONFIG.DEPTH;

    collector.addMany("bid_price_offset_", priceOffsetsBid, (idx) => `bid_price_offset_${idx + 1}`);
    collector.addMany("ask_price_offset_", priceOffsetsAsk, (idx) => `ask_price_offset_${idx + 1}`);
    collector.addMany("bid_log_vol_", logVolBid, (idx) => `bid_log_vol_${idx + 1}`);
    collector.addMany("ask_log_vol_", logVolAsk, (idx) => `ask_log_vol_${idx + 1}`);
}

function addDepthAndImbalance(ms, bid, ask, normalizer, collector) {
    const bidVolumes = bid.map((x) => Math.max(x.v, 0));
    const askVolumes = ask.map((x) => Math.max(x.v, 0));

    IMBALANCE_K.forEach((k) => {
        const sumBid = sumTopK(bidVolumes, k);
        const sumAsk = sumTopK(askVolumes, k);
        const num = sumBid - sumAsk;
        const den = sumBid + sumAsk || 1e-6;
        const imbalance = clip(num / den, -1, 1);
        collector.add(`imbalance_top${k}`, imbalance);
        collector.add(`imbalance_top${k}_z`, normalizer.zImbalance(ms, imbalance, k));
    });

    DEPTH_K.forEach((k) => {
        const cumBid = log1p(sumTopK(bidVolumes, k));
        const cumAsk = log1p(sumTopK(askVolumes, k));
        collector.add(`cum_bid_top${k}_log`, cumBid);
        collector.add(`cum_ask_top${k}_log`, cumAsk);
    });

    DEPTH_RATIO_K.forEach((k) => {
        const sumBid = sumTopK(bidVolumes, k);
        const sumAsk = sumTopK(askVolumes, k);
        const ratio = sumAsk === 0 ? (sumBid > 0 ? 10 : 1) : sumBid / sumAsk;
        const depthRatio = clip(ratio, 0, 10);
        collector.add(`depth_ratio_top${k}`, depthRatio);
        collector.add(`depth_ratio_top${k}_z`, normalizer.zDepthRatio(ms, depthRatio, k));
    });
}

function addShapeMetrics(bid, ask, normalizer, collector) {
    const bidVolumes = bid.map((x) => Math.max(x.v, 0));
    const askVolumes = ask.map((x) => Math.max(x.v, 0));
    const bidSlope = regressionSlope(bidVolumes);
    const askSlope = regressionSlope(askVolumes);
    const bidConvex = quadraticSecondCoeff(bidVolumes);
    const askConvex = quadraticSecondCoeff(askVolumes);

    collector.add("bid_volume_slope", bidSlope);
    collector.add("ask_volume_slope", askSlope);
    collector.add("bid_volume_convexity", bidConvex);
    collector.add("ask_volume_convexity", askConvex);
}

function addVolumeDynamics({ ms, dvolBidTop, dvolAskTop, cogBid, cogAsk, cogBidSpeed, cogAskSpeed }, normalizer, collector) {
    const bidArray = Array.isArray(dvolBidTop) ? dvolBidTop : [];
    const askArray = Array.isArray(dvolAskTop) ? dvolAskTop : [];

    const combined = [...bidArray, ...askArray];
    const normalized = normalizer.zArray(ms, combined, "dvol");
    for (let i = 0; i < bidArray.length; i++) {
        collector.add(`bid_dvol_top${i + 1}_z`, normalized[i]);
    }
    for (let i = 0; i < askArray.length; i++) {
        collector.add(`ask_dvol_top${i + 1}_z`, normalized[bidArray.length + i]);
    }

    DVOL_SUM_K.forEach((k) => {
        const sumBid = sumTopK(bidArray, k);
        const sumAsk = sumTopK(askArray, k);
        collector.add(`bid_dvol_sum_top${k}`, normalizer.zDvol(ms, sumBid));
        collector.add(`ask_dvol_sum_top${k}`, normalizer.zDvol(ms, sumAsk));
    });

    collector.add("cog_bid", cogBid);
    collector.add("cog_ask", cogAsk);
    collector.add("cog_bid_z", normalizer.zCog(ms, cogBid, "bid"));
    collector.add("cog_ask_z", normalizer.zCog(ms, cogAsk, "ask"));
    collector.add("cog_bid_speed", cogBidSpeed);
    collector.add("cog_ask_speed", cogAskSpeed);
    collector.add("cog_bid_speed_z", normalizer.zCogSpeed(ms, cogBidSpeed, "bid"));
    collector.add("cog_ask_speed_z", normalizer.zCogSpeed(ms, cogAskSpeed, "ask"));
}

function addGapSignals(bid, ask, collector) {
    const bidVolumes = bid.map((x) => Math.max(x.v, 0));
    const askVolumes = ask.map((x) => Math.max(x.v, 0));
    const gapThreshold = CONFIG.GAP_VOLUME_THRESHOLD;
    const largeClusterThreshold = CONFIG.GAP_LARGE_VOLUME_THRESHOLD || gapThreshold;
    const lookupLevels = CONFIG.GAP_LOOKUP_LEVELS;

    GAP_ZERO_WINDOWS.forEach((window) => {
        collector.add(`bid_zero_levels_top${window}`, countZeroVolumes(bidVolumes, window, gapThreshold));
        collector.add(`ask_zero_levels_top${window}`, countZeroVolumes(askVolumes, window, gapThreshold));
    });

    collector.add("bid_large_cluster_distance", distanceToLargeCluster(bidVolumes, lookupLevels, largeClusterThreshold));
    collector.add("ask_large_cluster_distance", distanceToLargeCluster(askVolumes, lookupLevels, largeClusterThreshold));
}

function addTradeZones(ms, zones, normalizer, collector) {
    zones.forEach((zone, idx) => {
        const zoneLabel = `zone_${idx + 1}`;
        const bidCount = normalizer.zTrCnt(ms, normalizer.clipTradeCnt(zone.bid_count_per_sec));
        const bidVol = normalizer.zTrVol(ms, normalizer.clipTradeVol(zone.bid_vol_per_sec));
        const askCount = normalizer.zTrCnt(ms, normalizer.clipTradeCnt(zone.ask_count_per_sec));
        const askVol = normalizer.zTrVol(ms, normalizer.clipTradeVol(zone.ask_vol_per_sec));
        const bidVwap = normalizer.zVwapOff(ms, zone.bid_vwap_off || 0);
        const askVwap = normalizer.zVwapOff(ms, zone.ask_vwap_off || 0);

        collector.add(`${zoneLabel}_bid_count_z`, bidCount);
        collector.add(`${zoneLabel}_bid_vol_z`, bidVol);
        collector.add(`${zoneLabel}_bid_vwap_off_z`, bidVwap);
        collector.add(`${zoneLabel}_ask_count_z`, askCount);
        collector.add(`${zoneLabel}_ask_vol_z`, askVol);
        collector.add(`${zoneLabel}_ask_vwap_off_z`, askVwap);
    });
}

function addTradeSizeBins(ms, aggs, normalizer, collector) {
    const clipCnt = aggs.sizeBinsCountPerSec.map(normalizer.clipTradeCnt.bind(normalizer));
    const clipVol = aggs.sizeBinsVolPerSec.map(normalizer.clipTradeVol.bind(normalizer));

    const countsZ = normalizer.zArray(ms, clipCnt, "trCnt");
    const volumesZ = normalizer.zArray(ms, clipVol, "trVol");

    collector.addMany("size_bin_", countsZ, (idx) => `size_bin_${idx + 1}_count_z`);
    collector.addMany("size_bin_", volumesZ, (idx) => `size_bin_${idx + 1}_volume_z`);
}

function addTradeQuantiles(ms, aggs, normalizer, collector) {
    const { quantileBinsCount = [], quantileBinsVol = [] } = aggs;
    if (!quantileBinsCount.length || !quantileBinsVol.length) {
        for (let i = 0; i < CONFIG.TRADE_QUANTILE_BINS; i++) {
            collector.add(`trade_quantile_bin_${i + 1}_count_z`, 0);
            collector.add(`trade_quantile_bin_${i + 1}_volume_z`, 0);
        }
        return;
    }

    const countsZ = normalizer.zArray(ms, quantileBinsCount.map(normalizer.clipTradeCnt.bind(normalizer)), "trCnt");
    const volumesZ = normalizer.zArray(ms, quantileBinsVol.map(normalizer.clipTradeVol.bind(normalizer)), "trVol");

    countsZ.forEach((value, idx) => collector.add(`trade_quantile_bin_${idx + 1}_count_z`, value));
    volumesZ.forEach((value, idx) => collector.add(`trade_quantile_bin_${idx + 1}_volume_z`, value));
}

function sumTopK(values, k) {
    if (!values || !values.length) return 0;
    const limit = Math.min(k, values.length);
    let sum = 0;
    for (let i = 0; i < limit; i++) sum += values[i] || 0;
    return sum;
}

function regressionSlope(volumes) {
    const n = volumes.length;
    if (n < 2) return 0;
    let sumX = 0;
    let sumY = 0;
    let sumXX = 0;
    let sumXY = 0;
    for (let i = 0; i < n; i++) {
        const x = i + 1;
        const y = Math.log1p(Math.max(volumes[i], 0));
        sumX += x;
        sumY += y;
        sumXX += x * x;
        sumXY += x * y;
    }
    const denom = n * sumXX - sumX * sumX;
    if (Math.abs(denom) < 1e-8) return 0;
    const slope = (n * sumXY - sumX * sumY) / denom;
    return clip(slope, -5, 5);
}

function quadraticSecondCoeff(volumes) {
    const n = volumes.length;
    if (n < 3) return 0;

    let sum1 = 0, sumX = 0, sumX2 = 0, sumX3 = 0, sumX4 = 0;
    let sumY = 0, sumXY = 0, sumX2Y = 0;
    for (let i = 0; i < n; i++) {
        const x = i + 1;
        const y = Math.log1p(Math.max(volumes[i], 0));
        const x2 = x * x;
        sum1 += 1;
        sumX += x;
        sumX2 += x2;
        sumX3 += x2 * x;
        sumX4 += x2 * x2;
        sumY += y;
        sumXY += x * y;
        sumX2Y += x2 * y;
    }

    const denom =
        sum1 * (sumX2 * sumX4 - sumX3 * sumX3) -
        sumX * (sumX * sumX4 - sumX2 * sumX3) +
        sumX2 * (sumX * sumX3 - sumX2 * sumX2);

    if (Math.abs(denom) < 1e-8) return 0;

    const cNumerator =
        sum1 * (sumX2 * sumX2Y - sumX3 * sumXY) -
        sumX * (sumX * sumX2Y - sumX2 * sumXY) +
        sumX2 * (sumX * sumXY - sumX2 * sumY);

    const c = cNumerator / denom;
    return clip(c, -1, 1);
}

function countZeroVolumes(volumes, window, threshold) {
    const limit = Math.min(window, volumes.length);
    const thresh = threshold ?? 0;
    let count = 0;
    for (let i = 0; i < limit; i++) {
        if ((volumes[i] ?? 0) <= thresh) count++;
    }
    return count;
}

function distanceToLargeCluster(volumes, lookupLevels, threshold) {
    const limit = Math.min(lookupLevels, volumes.length);
    const thresh = threshold ?? 0;
    for (let i = 0; i < limit; i++) {
        if ((volumes[i] ?? 0) > thresh) return i + 1;
    }
    return 0;
}
