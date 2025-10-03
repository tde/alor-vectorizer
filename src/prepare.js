// prepare.js - Модуль для подготовки данных из дамп файлов
import fs from "fs";
import path from "path";
import readline from "node:readline";
import { CONFIG } from "./config.js";
import { createPipeline } from "./agragate/pipeline.js";
import { FeatureSink } from "./agragate/FeatureSink.js";
import { PriceSink } from "./agragate/PriceSink.js";

/**
 * Основная функция модуля prepare
 * @param {string[]} args - аргументы командной строки
 */
export async function runPrepare(args) {
  try {
    console.log('[i] Запуск модуля подготовки данных...');
    
    // Парсим аргументы
    const { date, symbol } = parsePrepareArgs(args);
    console.log(`[i] Дата: ${date}`);
    console.log(`[i] Символ: ${symbol}`);
    
    // Читаем и парсим файл
    console.log('[i] Чтение и парсинг файла...');
    const summary = await prepareFeaturesFromDumpFile(symbol, date);

    if (summary) {
      const [rows, cols] = summary.featureShape;
      console.log(`[i] Получено срезов стакана: ${rows}`);
      console.log(`[i] Размерность признакового вектора: ${cols}`);
      if (summary.featureNames.length) {
        console.log('[i] Файл с перечнем признаков записан.');
      }
    }
    
    console.log(`[i] Успешно обработано`);
  } catch (error) {
    console.error('[!] Ошибка в модуле подготовки данных:', error.message);
    throw error;
  }
}

/**
 * Формирует имя файла на основе символа и даты
 * @param {string} symbol - символ инструмента
 * @param {string} date - дата в формате yyyy-mm-dd
 * @returns {string} имя файла
 */
export function buildFileName(symbol, date) {
    return `${symbol}_data_${date}.json`;
}
  
  /**
   * Читает и парсит файл дампа построчно
   * @param {string} filePath - путь к файлу
   * @returns {Array} массив объектов из JSON строк
   */
async function prepareFeaturesFromDumpFile(symbol, date) {
    // Формируем имя файла
    const fileName = buildFileName(symbol, date);

    // Путь к файлу в папке data
    const filePath = path.join(CONFIG.DATA_DIR, fileName);
    console.log(`[i] Путь к файлу: ${filePath}`);

    if (!fs.existsSync(filePath)) {
      throw new Error(`Файл не найден: ${filePath}`);
    }
    
    const pipeline = createPipeline();
    const featSink = new FeatureSink();
    const priceSink = new PriceSink();

    let featureNames = [];
    let lineNumber = 0;

    const input = fs.createReadStream(filePath, "utf8");
    const rl = readline.createInterface({ input, crlfDelay: Infinity });

    try {
      for await (const rawLine of rl) {
        lineNumber++;
        const line = rawLine.trim();
        if (!line) continue;

        let result;
        try {
          result = pipeline.feedLine(line);
        } catch (error) {
          throw new Error(`Ошибка при обработке строки ${lineNumber}: ${error.message}`);
        }

        if (result && result.data) {
          featSink.add(result.data.vector);
          priceSink.add(result.ms, result.mid);
          if (!featureNames.length && Array.isArray(result.data.featureNames)) {
            featureNames = [...result.data.featureNames];
          }
        }
      }
    } finally {
      rl.close();
      input.close?.();
    }

    const featuresPath = path.join(CONFIG.DATA_DIR, `${symbol}_${date}_features.npy`);
    const pricesPath = path.join(CONFIG.DATA_DIR, `${symbol}_${date}_prices.csv`);
    const featureListPath = path.join(CONFIG.DATA_DIR, `${symbol}_${date}_feature_names.txt`);

    featSink.saveNPY(featuresPath);
    priceSink.saveCSV(pricesPath);

    if (featureNames.length) {
      fs.writeFileSync(featureListPath, featureNames.join("\n"), "utf8");
    }

    return {
      featureNames,
      featureShape: featSink.shape(),
    };
} 

/**
 * Парсит аргументы командной строки для параметра --prepare
 * @param {string[]} args - аргументы командной строки
 * @returns {Object} объект с параметрами date и symbol
 */
function parsePrepareArgs(args) {
    const dateIndex = args.indexOf('--date');
    
    if (dateIndex === -1 || dateIndex === args.length - 1) {
      throw new Error('Параметр --date обязателен и должен содержать дату в формате yyyy-mm-dd');
    }
    
    const date = args[dateIndex + 1];
    
    // Проверяем формат даты yyyy-mm-dd
    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateRegex.test(date)) {
      throw new Error('Дата должна быть в формате yyyy-mm-dd');
    }
    
    // Получаем символ только из конфига
    const symbol = CONFIG.SYMBOL;
    if (!symbol) {
      throw new Error('Символ не найден в конфиге');
    }
    
    return { date, symbol };
  }
