// main.js - Main entry point for the application
import { CONFIG } from "./config.js";
import { runTradesStats } from "./tradesStat.js";
import { runDump } from "./dump.js";
import { runPrepare } from "./prepare.js";

async function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--trades-stat')) {
    console.log('[i] Запуск модуля статистики сделок...');
    await runTradesStats();
  } else if (args.includes('--dump')) {
    console.log('[i] Запуск модуля дампа данных...');
    await runDump();
  } else if (args.includes('--prepare')) {
    console.log('[i] Запуск модуля подготовки данных...');
    await runPrepare(args);
  } else {
    console.log('[i] Доступные модули:');
    console.log('[i]   --trades-stat  - Статистика сделок');
    console.log('[i]   --dump         - Дамп данных');
    console.log('[i]   --prepare      - Подготовка данных из дамп файлов');
    console.log('[i] Примеры использования:');
    console.log('[i]   node src/main.js --trades-stat');
    console.log('[i]   node src/main.js --dump');
    console.log('[i]   node src/main.js --prepare --date 2025-08-27');
  }
}

// Run main function
main().catch(err => {
  console.error("[!] Ошибка:", err);
  process.exit(1);
});
