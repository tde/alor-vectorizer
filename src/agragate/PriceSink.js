import fs from "node:fs";

export class PriceSink {
    constructor() {
      this.ms = [];
      this.mid = [];
    }
    add(ms, mid) {
      this.ms.push(ms|0);
      this.mid.push(Number(mid));
    }
    saveCSV(path = "./prices.csv") {
      const lines = ["ms,mid"];
      for (let i = 0; i < this.ms.length; i++) lines.push(`${this.ms[i]},${this.mid[i]}`);
      fs.writeFileSync(path, lines.join("\n"));
    }
    saveNPY(path = "./prices.npy") {
      // простой .npy не умеет словари, поэтому сохраним JSON рядом:
      fs.writeFileSync(path.replace(/\.npy$/,"") + ".json",
        JSON.stringify({ ms: this.ms, mid: this.mid }, null, 2), "utf8");
    }
}