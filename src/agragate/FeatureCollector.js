export class FeatureCollector {
  constructor() {
    this._values = [];
    this._names = [];
  }

  add(name, value) {
    this._names.push(String(name));
    this._values.push(Number.isFinite(value) ? value : 0);
  }

  addMany(prefix, values, formatter = (idx) => `${prefix}${idx}`) {
    for (let i = 0; i < values.length; i++) {
      const name = typeof formatter === "function" ? formatter(i, values[i]) : `${prefix}${i}`;
      this.add(name, values[i]);
    }
  }

  names() {
    return this._names;
  }

  toFloat32() {
    return Float32Array.from(this._values);
  }
}
