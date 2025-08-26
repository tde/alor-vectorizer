// main.js - Main entry point for the application
import { CONFIG } from "./config.js";
import { runTradesStats } from "./tradesStat.js";

async function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--trades-stat')) {
    console.log('[i] Запуск модуля статистики сделок...');
    await runTradesStats();
  } else {
    console.log('[i] Модуль статистики сделок не запущен');
    console.log('[i] Для запуска используйте: node src/main.js --trades-stat');
    console.log('[i] Дополнительные модули будут добавлены позже');
  }
}

// Run main function
main().catch(err => {
  console.error("[!] Ошибка:", err);
  process.exit(1);
});
