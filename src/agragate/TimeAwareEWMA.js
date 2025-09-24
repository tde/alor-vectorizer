// time-aware EWMA mean/std для SWN: не храним окно, всё онлайн
export class TimeAwareEWMA {
    constructor(tauSec, eps = 1e-8) {
      this.tau = tauSec;
      this.eps = eps;
      this.mean = null;
      this.m2 = 0;     // для дисперсии
      this.lastTs = null;
    }
    _alpha(dtSec) {
      if (!Number.isFinite(dtSec) || dtSec <= 0) return 0;
      return 1 - Math.exp(-dtSec / this.tau);
    }
    // вернёт z-score после обновления
    update(tsMs, x) {
      if (this.lastTs == null) {
        this.lastTs = tsMs;
        this.mean = x;
        this.m2 = 0;
        return 0; // первый z = 0
      }
      const dtSec = (tsMs - this.lastTs) / 1000;
      const a = this._alpha(dtSec);
      this.lastTs = tsMs;
  
      // EWMA обновление среднего
      const prevMean = this.mean;
      const mean = a * x + (1 - a) * prevMean;
  
      // EWMA дисперсии (экспоненц. сглаживание квадр. отклонения)
      const diff = x - prevMean;
      const varInc = a * (diff * diff);
      const var_ = (1 - a) * this.m2 + varInc;
  
      this.mean = mean;
      this.m2 = var_;
  
      const std = Math.sqrt(Math.max(var_, this.eps));
      return (x - mean) / (std + this.eps);
    }
  }
  