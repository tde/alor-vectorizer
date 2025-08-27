// dump.js - Модуль для дампа данных через WebSocket
import { AlorWS } from "./wsClient.js";
import { CONFIG } from "./config.js";
import fs from 'fs';
import path from 'path';
import writeFileAtomic from 'write-file-atomic';

// Кол-во строк в буфере при превышении которого сохраняем в файл
const MAX_BUFFER_SIZE = 200;

let wsClient = null;
let isRunning = false;

// Единый массив для накопления всех данных (сделки + стакан)
let dataBuffer1 = [];
let dataBuffer2 = [];
let dataBufferCurrent = dataBuffer1
let currentIndex = 0;

// признак того, что идет сохранение в файл
let isSaving = false;

// Счетчик сообщений для логирования
let messageCounter = 0;

/**
 * Основная функция дампа данных
 */
export async function runDump() {
  console.log('[i] Запуск модуля дампа данных...');
  
  try {
    console.log('[i] Настройки дампа:');
    console.log(`  - API URL: ${CONFIG.BASE_URL}`);
    console.log(`  - WebSocket URL: ${CONFIG.WS_URL}`);
    console.log(`  - Биржа: ${CONFIG.EXCHANGE}`);
    console.log(`  - Инструмент: ${CONFIG.SYMBOL}`);
    console.log(`  - Интервал сохранения: ${CONFIG.FLUSH_EVERY_MS}ms`);
    console.log(`  - Папка данных: ${CONFIG.DATA_DIR}`);
    
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
//для теста
  console.log(`dataBuffer1 length = ${dataBuffer1.length}`);
  console.log(`dataBuffer2 length = ${dataBuffer2.length}`);
  console.log(`datBuffer length = ${datBuffer.length}`);

  if (isSaving) {
    throw new Error('Сохранение в файл уже идет');
  }

  try {
    isSaving = true;
    const timestamp = new Date();
    const startSavingTime = timestamp.getTime();

    const dateStr = timestamp.toISOString().split('T')[0]; // YYYY-MM-DD
    const fileName = `${CONFIG.SYMBOL}_data_${dateStr}.json`;
    const filePath = path.join(CONFIG.DATA_DIR, fileName);

    // Формируем данные для записи - каждый элемент на новой строке
    let dataToWrite = '';
    
    // Добавляем каждую строку из буфера на новой строке
    datBuffer.forEach((item, index) => {
      dataToWrite += item + '\n';
    });
    
    // Атомарно дописываем в конец файла с callback
    writeFileAtomic(filePath, dataToWrite, {
      encoding: 'utf8',
      mode: 0o644,
      flag: 'a' // Флаг для дописывания в конец
    }, (err) => {
      if (err) {
        console.error('[!] Ошибка при записи файла:', err);
      } else {
        console.log(`[💾] Сохранено ${datBuffer.length} записей в ${fileName}, время сохранения: ${new Date().getTime() - startSavingTime}ms`);
      }
      datBuffer.length = 0;

      console.log(`dataBuffer1 length = ${dataBuffer1.length}`);
      console.log(`dataBuffer2 length = ${dataBuffer2.length}`);
      console.log(`datBuffer length = ${datBuffer.length}`);

      isSaving = false;
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
    // Обрабатываем различные типы сообщений
    if (!message.data) {
      // Логируем неизвестные типы сообщений для отладки
      console.log(`[i] Получено сообщение: ${message.opcode || 'unknown'}`);
      return;
    }

    const rec = JSON.stringify(message, null, ''); // Убираем отступы и переносы строк

    if (dataBufferCurrent.length % 100 === 0) {
      console.log(`[i] Буфер содержит ${dataBufferCurrent.length} сообщений`);
    }

    // проверяем размер буфера
    if (dataBufferCurrent.length > MAX_BUFFER_SIZE) {
      console.log(`[i] Буфер переполнен, сохраняем в файл`);
      let bufferToSave = swapDataBuffers();

      console.log(`dataBufferCurrent length = ${dataBufferCurrent.length}`);

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
 * Обработка сообщений о сделках
 * @param {Object} message - Сообщение о сделках
 */
function handleTradesMessage(message) {
  if (message.data && Array.isArray(message.data)) {
    message.data.forEach(trade => {
      // Форматируем время сделки
      const tradeTime = trade.time ? new Date(trade.time * 1000).toLocaleTimeString() : 'N/A';
      
      // Увеличиваем счетчик сообщений
      messageCounter++;
      
      // Выводим сообщение каждые 100 полученных сообщений
      if (messageCounter % 100 === 0) {
        console.log(`[TRADE] Получено ${messageCounter} сообщений. Последняя сделка: ${trade.symbol || CONFIG.SYMBOL}: ${trade.qty} @ ${trade.price} (${tradeTime})`);
      }
      
      // Добавляем в общий буфер с типом данных
      dataBuffer.push({
        type: 'trade',
        data: trade,
        timestamp: new Date().toISOString(),
        saved: false // Помечаем как несохраненную
      });
    });
  } else if (message.data && typeof message.data === 'object') {
    // Одиночная сделка
    const trade = message.data;
    const tradeTime = trade.time ? new Date(trade.time * 1000).toLocaleTimeString() : 'N/A';
    
    // Увеличиваем счетчик сообщений
    messageCounter++;
    
    // Выводим сообщение каждые 100 полученных сообщений
    if (messageCounter % 100 === 0) {
      console.log(`[TRADE] Получено ${messageCounter} сообщений. Последняя сделка: ${trade.symbol || CONFIG.SYMBOL}: ${trade.qty} @ ${trade.price} (${tradeTime})`);
    }
    
    // Добавляем в общий буфер с типом данных
    dataBuffer.push({
      type: 'trade',
      data: trade,
      timestamp: new Date().toISOString(),
      saved: false // Помечаем как несохраненную
    });
  }
}

/**
 * Обработка сообщений о стакане заявок
 * @param {Object} message - Сообщение о стакане
 */
function handleOrderBookMessage(message) {
  if (message.data) {
    console.log(`[ORDERBOOK] ${CONFIG.SYMBOL}: ${message.data.bids?.length || 0} bids, ${message.data.asks?.length || 0} asks`);
    
    // Добавляем в общий буфер с типом данных
    dataBuffer.push({
      type: 'orderbook',
      data: message.data,
      timestamp: new Date().toISOString(),
      saved: false // Помечаем как несохраненную
    });
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
