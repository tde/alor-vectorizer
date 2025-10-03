import { CONFIG } from "../config.js";
import { TimeAwareEWMA } from "./TimeAwareEWMA.js";
import { clip } from "../utils.js";

export class Normalizer {
  constructor() {
    // SWN для быстрых скаляров
    this.s_microDelta = new TimeAwareEWMA(CONFIG.TAU_SWN_FAST);
    this.s_microSpeed = new TimeAwareEWMA(CONFIG.TAU_SWN_FAST);
    this.s_dvol = new TimeAwareEWMA(CONFIG.TAU_SWN_FAST);
    this.s_trCnt = new TimeAwareEWMA(CONFIG.TAU_SWN_FAST);
    this.s_trVol = new TimeAwareEWMA(CONFIG.TAU_SWN_FAST);
    this.s_vwapOff = new TimeAwareEWMA(CONFIG.TAU_SWN_FAST);
    this.s_spreadSpeed = new TimeAwareEWMA(CONFIG.TAU_SWN_FAST);
    this.dynamicStats = new Map();
  }

  zMicroDelta(ms, x) { return clip(this.s_microDelta.update(ms, x), -CONFIG.CLIP_Z_SCORE, CONFIG.CLIP_Z_SCORE); }
  zMicroSpeed(ms, x) { return clip(this.s_microSpeed.update(ms, x), -CONFIG.CLIP_Z_SCORE, CONFIG.CLIP_Z_SCORE); }
  zDvol(ms, x)       { return clip(this.s_dvol.update(ms, x), -CONFIG.CLIP_Z_SCORE, CONFIG.CLIP_Z_SCORE); }
  zTrCnt(ms, x)      { return clip(this.s_trCnt.update(ms, x), -CONFIG.CLIP_Z_SCORE, CONFIG.CLIP_Z_SCORE); }
  zTrVol(ms, x)      { return clip(this.s_trVol.update(ms, x), -CONFIG.CLIP_Z_SCORE, CONFIG.CLIP_Z_SCORE); }
  zVwapOff(ms, x)    { return clip(this.s_vwapOff.update(ms, x), -CONFIG.CLIP_Z_SCORE, CONFIG.CLIP_Z_SCORE); }
  zSpreadSpeed(ms, x){ return clip(this.s_spreadSpeed.update(ms, x), -CONFIG.CLIP_Z_SCORE, CONFIG.CLIP_Z_SCORE); }
  zCog(ms, x, tag = "") { return this._zByKey(ms, x, `cog:${tag}`); }
  zCogSpeed(ms, x, tag = "") { return this._zByKey(ms, x, `cogSpeed:${tag}`); }
  zImbalance(ms, x, tag = "") { return this._zByKey(ms, x, `imbalance:${tag}`); }
  zDepthRatio(ms, x, tag = "") { return this._zByKey(ms, x, `depthRatio:${tag}`); }

  // массив применяем через общий стэт (быстро и стабильно)
  zArray(ms, arr, kind) {
    const out = new Array(arr.length);
    for (let i = 0; i < arr.length; i++) {
      const x = arr[i];
      switch (kind) {
        case "dvol": out[i] = this.zDvol(ms, x); break;
        case "trCnt": out[i] = this.zTrCnt(ms, x); break;
        case "trVol": out[i] = this.zTrVol(ms, x); break;
        default: out[i] = x;
      }
    }
    return out;
  }

  clipTradeCnt(x){ return Math.min(x, CONFIG.CLIP_TRADE_COUNT_PER_SEC); }
  clipTradeVol(x){ return Math.min(x, CONFIG.CLIP_TRADE_VOL_PER_SEC); }

  _zByKey(ms, value, key) {
    if (!this.dynamicStats.has(key)) {
      this.dynamicStats.set(key, new TimeAwareEWMA(CONFIG.TAU_SWN_FAST));
    }
    const stat = this.dynamicStats.get(key);
    return clip(stat.update(ms, value), -CONFIG.CLIP_Z_SCORE, CONFIG.CLIP_Z_SCORE);
  }
}
