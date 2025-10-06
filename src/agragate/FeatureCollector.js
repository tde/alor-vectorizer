export class FeatureCollector {
  constructor(enableNames = true) {
    this._values = [];
    this._names = enableNames ? [] : null;
  }

  add(name, value) {
    if (this._names) {
      this._names.push(name !== undefined ? String(name) : "");
    }
    this._values.push(Number.isFinite(value) ? value : 0);
  }

  addMany(prefix, values, formatter = (idx) => `${prefix}${idx}`) {
    const collectNames = Boolean(this._names);
    for (let i = 0; i < values.length; i++) {
      const name = collectNames
        ? (typeof formatter === "function" ? formatter(i, values[i]) : `${prefix}${i}`)
        : undefined;
      this.add(name, values[i]);
    }
  }

  names() {
    return this._names ?? [];
  }

  toFloat32() {
    return Float32Array.from(this._values);
  }
}
