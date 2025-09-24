import fs from "node:fs";

/**
 * Санитайзер: заменяем NaN/±Inf на 0, приводим к float32.
 */
function sanitizeToFloat32(arrLike) {
  const out = new Float32Array(arrLike.length);
  for (let i = 0; i < arrLike.length; i++) {
    const x = arrLike[i];
    out[i] = Number.isFinite(x) ? x : 0;
  }
  return out;
}

/**
 * Минималистичный .npy writer (NumPy v1.0) для 2D массива float32 (row-major).
 * shape: (rows, cols)
 */
function writeNpyFloat32(filePath, rows, cols, dataFloat32) {
  // Magic + version
  const magic = Buffer.from([0x93, 0x4E, 0x55, 0x4D, 0x50, 0x59]); // \x93NUMPY
  const ver = Buffer.from([0x01, 0x00]); // v1.0

  // Header dict (ASCII), pad to 16-byte alignment
  const descr = "<f4"; // little-endian float32
  const fortran = "False";
  const shapeStr = `(${rows}, ${cols})`; // tuple syntax
  let header = `{'descr': '${descr}', 'fortran_order': ${fortran}, 'shape': ${shapeStr}, }`;
  // Pad with spaces and newline to 16-byte alignment
  const baseLen = magic.length + ver.length + 2; // +2 for header_len field
  let headerLen = header.length;
  const padLen = (16 - ((baseLen + headerLen) % 16)) % 16;
  header = header + " ".repeat(padLen) + "\n";
  headerLen = header.length;

  const headerLenBuf = Buffer.alloc(2);
  headerLenBuf.writeUInt16LE(headerLen, 0);

  // Write file
  const fd = fs.openSync(filePath, "w");
  try {
    fs.writeSync(fd, magic);
    fs.writeSync(fd, ver);
    fs.writeSync(fd, headerLenBuf);
    fs.writeSync(fd, Buffer.from(header, "ascii"));
    // Data: row-major float32 little-endian
    const buf = Buffer.from(dataFloat32.buffer, dataFloat32.byteOffset, dataFloat32.byteLength);
    fs.writeSync(fd, buf);
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * FeatureSink — копит строки фич и сохраняет CSV / NPY.
 */
export class FeatureSink {
  /**
   * @param {number|null} featureDim — ожидаемая длина вектора (можно null, чтобы определить по первой записи)
   */
  constructor(featureDim = null) {
    this._featureDim = featureDim;
    this._rows = []; // храним Float32Array по одной строке
  }

  /**
   * Добавить вектор признаков.
   * @param {number[]|Float32Array} feat
   */
  add(feat) {
    if (!feat || !feat.length) return;
    if (this._featureDim == null) this._featureDim = feat.length;
    if (feat.length !== this._featureDim) {
      throw new Error(`Feature length mismatch: got ${feat.length}, expected ${this._featureDim}`);
    }
    this._rows.push(sanitizeToFloat32(feat));
  }

  /**
   * Сохранить в CSV (без заголовка). Каждая строка — одна запись, значения разделены запятой.
   */
  saveCSV(filePath) {
    const { _rows, _featureDim } = this;
    if (!_rows.length) return;
    const out = _rows.map(r => Array.from(r).join(",")).join("\n") + "\n";
    fs.writeFileSync(filePath, out, "utf8");

    console.log(`[i] Сохранено в CSV: ${filePath}`);
  }

  /**
   * Сохранить в NumPy .npy (float32, shape=(N,F)), пригодно для Python/DeepLOB.
   */
  saveNPY(filePath) {
    const { _rows, _featureDim } = this;
    if (!_rows.length) return;
    const N = _rows.length;
    const F = _featureDim;
    const big = new Float32Array(N * F);
    for (let i = 0; i < N; i++) {
      big.set(_rows[i], i * F);
    }
    writeNpyFloat32(filePath, N, F, big);
  }

  /**
   * Очистить буфер.
   */
  reset() {
    this._rows = [];
  }

  /**
   * Текущая форма данных (N, F)
   */
  shape() {
    return [this._rows.length, this._featureDim ?? 0];
  }
}
