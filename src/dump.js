// dump.js - Модуль для дампа данных через WebSocket
import { AlorWS } from "./wsClient.js";
import { CONFIG } from "./config.js";
import fs from 'fs';
import path from 'path';
import writeFileAtomic from 'write-file-atomic';
import { DateTime } from 'luxon';

// Кол-во строк в буфере при превышении которого сохраняем в файл
const MAX_BUFFER_SIZE = 500;

let wsClient = null;
let isRunning = false;

// Единый массив для накопления всех данных (сделки + стакан)
let dataBuffer1 = [];
let dataBuffer2 = [];
let dataBufferCurrent = dataBuffer1
let currentIndex = 0;

// признак того, что идет сохранение в файл
let isSaving = false;

/**
 * Форматирование текущего времени для логов
 * @returns {string} Отформатированное время в формате [YYYY-MM-DD HH:MM:SS]
 */
function getLogTimestamp() {
  const moscowTime = DateTime.now().setZone(CONFIG.TIMEZONE);
  return `[${moscowTime.toFormat('yyyy-MM-dd HH:mm:ss')}]`;
}

/**
 * Проверка, находится ли текущее время в торговых часах (МСК)
 * @returns {boolean} true если сейчас торговые часы
 */
function isTradingTime() {
  // Получаем текущее время в торговом часовом поясе
  const tradingTime = DateTime.now().setZone(CONFIG.TIMEZONE);
  const currentTimeMinutes = tradingTime.hour * 60 + tradingTime.minute;
  
  // Проверяем каждую торговую сессию
  for (const session of CONFIG.TRADING_SESSIONS) {
    if (currentTimeMinutes >= session.start && currentTimeMinutes <= session.end) {
      return true;
    }
  }
  
  return false;
}

/**
 * Основная функция дампа данных
 */
export async function runDump() {
  console.log('[i] Запуск модуля дампа данных...');
  
  try {
    console.log(`${getLogTimestamp()} [i] Настройки дампа:`);
    console.log(`  - API URL: ${CONFIG.BASE_URL}`);
    console.log(`  - WebSocket URL: ${CONFIG.WS_URL}`);
    console.log(`  - Биржа: ${CONFIG.EXCHANGE}`);
    console.log(`  - Инструмент: ${CONFIG.SYMBOL}`);
    console.log(`  - Интервал сохранения: ${CONFIG.FLUSH_EVERY_MS}ms`);
    console.log(`  - Папка данных: ${CONFIG.DATA_DIR}`);
    console.log('  - Торговые часы:');
    CONFIG.TRADING_SESSIONS.forEach((session, index) => {
      const startTime = `${Math.floor(session.start / 60)}:${String(session.start % 60).padStart(2, '0')}`;
      const endTime = `${Math.floor(session.end / 60)}:${String(session.end % 60).padStart(2, '0')}`;
      console.log(`    Сессия ${index + 1}: ${startTime} - ${endTime}`);
    });
    
    // Показываем текущее время в торговом часовом поясе
    const tradingTime = DateTime.now().setZone(CONFIG.TIMEZONE);
    console.log(`  - Текущее время (${CONFIG.TIMEZONE}): ${tradingTime.toLocaleString(DateTime.DATETIME_FULL)}`);
    console.log(`  - Торговые часы активны: ${isTradingTime() ? 'ДА' : 'НЕТ'}`);
    
    // Создаем папку для данных, если её нет
    await ensureDataDirectory();
    
    // Создаем WebSocket клиент
    wsClient = new AlorWS();
    
    // Подключаемся и подписываемся на данные
    await wsClient.connect(handleWebSocketMessage);
    
    // Обрабатываем сигналы завершения
    process.on('SIGINT', cleanup);
    process.on('SIGTERM', cleanup);
    
    isRunning = true;
    console.log('[i] WebSocket подключен. Ожидание сделок в реальном времени...');
    console.log('[i] Нажмите Ctrl+C для завершения');
    
  } catch (error) {
    console.error('[!] Ошибка при выполнении дампа:', error);
    throw error;
  }
}

/**
 * Создание папки для данных, если её нет
 */
async function ensureDataDirectory() {
  try {
    if (!fs.existsSync(CONFIG.DATA_DIR)) {
      fs.mkdirSync(CONFIG.DATA_DIR, { recursive: true });
      console.log(`[i] Создана папка для данных: ${CONFIG.DATA_DIR}`);
    }
  } catch (error) {
    console.error('[!] Ошибка при создании папки данных:', error);
    throw error;
  }
}

/**
 * Сохранение всех данных в один файл
 */
function saveDataToFile(datBuffer) {
  if (isSaving) {
    throw new Error('Сохранение в файл уже идет');
  }

  try {
    isSaving = true;
    const timestamp = DateTime.now();
    const startSavingTime = timestamp.toMillis();

    const dateStr = timestamp.toISODate(); // YYYY-MM-DD
    const fileName = `${CONFIG.SYMBOL}_data_${dateStr}.json`;
    const filePath = path.join(CONFIG.DATA_DIR, fileName);

    // Формируем данные для записи - каждый элемент на новой строке
    let dataToWrite = '';
    
    // Добавляем каждую строку из буфера на новой строке
    datBuffer.forEach((item, index) => {
      dataToWrite += item + '\n';
    });
    
    fs.appendFile(filePath, dataToWrite, (err) => {
      if (err) {
        console.error('[!] Ошибка при записи файла:', err);
      } else {
        console.log(`${getLogTimestamp()} [💾] Сохранено ${datBuffer.length} записей в ${fileName}, время сохранения: ${DateTime.now().toMillis() - startSavingTime}ms`);
      }

      datBuffer.length = 0;
      isSaving = false;

      //вывод лога текщего размера файла
      const fileSize = fs.statSync(filePath).size;
      console.log(`${getLogTimestamp()} [💾] Текущий размер файла: ${Math.round(fileSize/1000)} kB`);
    });

    
  } catch (error) {
    console.error('[!] Ошибка при сохранении данных:', error);
    isSaving = false;
  }
}

/**
 * Обработчик сообщений WebSocket
 * @param {Object} message - Сообщение от WebSocket
 */
function handleWebSocketMessage(message) {
  try {
    // Проверяем торговые часы
    if (!isTradingTime()) {
      // Игнорируем сообщения вне торговых часов
      return;
    }

    // Обрабатываем различные типы сообщений
    if (!message.data) {
      // Логируем неизвестные типы сообщений для отладки
      console.log(`[i] Получено сообщение: ${message.opcode || 'unknown'}`);
      return;
    }

    const rec = JSON.stringify(message, null, ''); // Убираем отступы и переносы строк

    if (dataBufferCurrent.length % 100 === 0) {
      console.log(`${getLogTimestamp()} [i] Буфер содержит ${dataBufferCurrent.length} сообщений`);
    }

    // проверяем размер буфера
    if (dataBufferCurrent.length >= MAX_BUFFER_SIZE) {
      console.log(`${getLogTimestamp()} [i] Буфер переполнен, сохраняем в файл`);
      let bufferToSave = swapDataBuffers();

      // добавляем в буфер
      dataBufferCurrent.push(rec);

      // сохраняем в файл и очищаем буфер
      saveDataToFile(bufferToSave);
    }
    else {
      dataBufferCurrent.push(rec);
    }
  } catch (error) {
    console.error('[!] Ошибка при обработке WebSocket сообщения:', error);
  }
}

function swapDataBuffers() {
  if (currentIndex === 0) {
    dataBufferCurrent = dataBuffer2;
    currentIndex = 1;

    return dataBuffer1;
  } 
  else if (currentIndex === 1) {
    dataBufferCurrent = dataBuffer1;
    currentIndex = 0;

    return dataBuffer2;
  } 
  else {
    throw new Error('Неизвестный индекс буфера');
  }
}

/**
 * Обработка сообщений об ошибках
 * @param {Object} message - Сообщение об ошибке
 */
function handleErrorMessage(message) {
  console.error('[!] WebSocket ошибка:', message.message || message);
}

/**
 * Очистка ресурсов
 */
function cleanup() {
  console.log('\n[i] Завершение работы...');
  
  // Сохраняем оставшиеся данные перед завершением
  if (dataBufferCurrent.length > 0) {
    console.log('[i] Сохранение оставшихся данных...');

    
    saveDataToFile(dataBufferCurrent);
  }
  
  if (wsClient) {
    // Закрываем WebSocket соединение
    if (wsClient.ws) {
      wsClient.ws.close(1000, 'Завершение работы');
    }
  }
  
  isRunning = false;
  console.log('[i] Модуль дампа остановлен');
  process.exit(0);
}
