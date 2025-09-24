// utils.js - Утилиты для работы с данными
import { DateTime } from 'luxon';
import { CONFIG } from './config.js';

/**
 * Преобразует timestamp в формат времени hh:mm:ss.SSS
 * @param {number} timestamp - timestamp в миллисекундах или секундах
 * @returns {string} время в формате hh:mm:ss.SSS
 */
export function formatTimestamp(timestamp) {
  // Определяем, в миллисекундах или секундах timestamp
  const ms = timestamp >= 1e12 ? timestamp : timestamp * 1000;
  
  const dt = DateTime.fromMillis(ms, { zone: 'utc' });
  
  return dt.toFormat('HH:mm:ss.SSS');
}

/**
 * Преобразует timestamp в формат времени с учетом часового пояса
 * @param {number} timestamp - timestamp в миллисекундах или секундах
 * @param {string} timezone - часовой пояс (по умолчанию из CONFIG.TIMEZONE)
 * @returns {string} время в формате hh:mm:ss.SSS
 */
export function formatTimestampWithTimezone(timestamp, timezone = CONFIG.TIMEZONE) {
  // Определяем, в миллисекундах или секундах timestamp
  const ms = timestamp >= 1e12 ? timestamp : timestamp * 1000;
  
  const dt = DateTime.fromMillis(ms, { zone: timezone });
  
  return dt.toFormat('HH:mm:ss.SSS');
}

/**
 * Преобразует timestamp в полный формат даты и времени
 * @param {number} timestamp - timestamp в миллисекундах или секундах
 * @returns {string} дата и время в формате yyyy-mm-dd hh:mm:ss.SSS
 */
export function formatFullTimestamp(timestamp) {
  // Определяем, в миллисекундах или секундах timestamp
  const ms = timestamp >= 1e12 ? timestamp : timestamp * 1000;
  
  const dt = DateTime.fromMillis(ms, { zone: 'utc' });
  
  return dt.toFormat('yyyy-MM-dd HH:mm:ss.SSS');
}

/**
 * Получает timestamp из объекта данных (поддерживает разные форматы)
 * @param {Object} data - объект данных
 * @returns {number} timestamp в миллисекундах
 */
export function extractTimestamp(data) {
  // Проверяем разные возможные поля с timestamp
  if (data.ms_timestamp) {
    return data.ms_timestamp;
  }
  
  if (data.timestamp) {
    // Если timestamp в секундах, конвертируем в миллисекунды
    return data.timestamp >= 1e12 ? data.timestamp : data.timestamp * 1000;
  }
  
  throw new Error('Timestamp не найден в данных');
}

// === Функции работы с московским временем (MSK) ===

/**
 * Преобразует timestamp в торговое время
 * @param {number} tsMs - timestamp в миллисекундах
 * @returns {DateTime} объект DateTime в торговом часовом поясе
 */
export function toMskDate(tsMs) {
  return DateTime.fromMillis(tsMs, { zone: CONFIG.TIMEZONE });
}

/**
 * Форматирует timestamp в торговое время в формате dd.MM.yyyy HH:mm
 * @param {number} tsMs - timestamp в миллисекундах
 * @returns {string} отформатированное время
 */
export function formatMsk(tsMs) {
  const d = toMskDate(tsMs);
  return d.toFormat('dd.MM.yyyy HH:mm');
}

/**
 * Проверяет, разрешена ли минута в торговом времени (09:00–23:58)
 * @param {number} tsMs - timestamp в миллисекундах
 * @returns {boolean} true если время разрешено
 */
export function isAllowedMskMinute(tsMs) {
  if (!Number.isFinite(tsMs)) return false;
  const tradingTime = DateTime.fromMillis(tsMs, { zone: CONFIG.TIMEZONE });
  const h = tradingTime.hour;
  const m = tradingTime.minute;
  
  if (h < 9) return false;            // before 09:00 — exclude
  if (h > 23) return false;           // safety check (won't happen)
  if (h < 23) return true;            // 09:00..22:59 — allow
  return m <= 58;                      // 23:00..23:58 — allow, 23:59 — exclude
}

/**
 * Проверяет, нужно ли пропустить час в торговом времени (00:00-08:59)
 * @param {number} tsMsHourStart - timestamp начала часа в миллисекундах
 * @returns {boolean} true если час нужно пропустить
 */
export function shouldSkipHourMsk(tsMsHourStart) {
  const tradingTime = DateTime.fromMillis(tsMsHourStart, { zone: CONFIG.TIMEZONE });
  const h = tradingTime.hour;
  return h < 9; // 00..08 — skip
}

// === Функции работы с датами ===

/**
 * Парсит дату в формате dd.MM.yyyy
 * @param {string} dateStr - строка с датой
 * @returns {DateTime|null} объект DateTime или null если не удалось распарсить
 */
export function parseDdMmYyyy(dateStr) {
  if (!dateStr) return null;
  const m = /^([0-3]?\d)\.([01]?\d)\.(\d{4})$/.exec(String(dateStr).trim());
  if (!m) return null;
  
  const dd = Number(m[1]);
  const MM = Number(m[2]);
  const yyyy = Number(m[3]);
  
  try {
    const dt = DateTime.fromObject({ year: yyyy, month: MM, day: dd, hour: 0, minute: 0, second: 0, millisecond: 0 });
    return dt.isValid ? dt : null;
  } catch (error) {
    return null;
  }
}

/**
 * Проверяет, является ли дата выходным днем
 * @param {DateTime} dateObj - объект DateTime
 * @returns {boolean} true если это выходной день
 */
export function isWeekend(dateObj) {
  const w = dateObj.weekday;
  return w === 7 || w === 6; // Sunday=7, Saturday=6 в Luxon
}

export const msToSec = (ms) => ms / 1000;

/**
 * Математические функции
 */
export const safeDiv = (num, den, def = 0) => (den !== 0 ? num / den : def);

export const log1p = (x) => Math.log(1 + Math.max(x, 0));

export const clip = (x, lo, hi) => Math.max(lo, Math.min(hi, x));

export const ewmaUpdate = (prev, x, alpha) =>
  prev == null ? x : alpha * x + (1 - alpha) * prev;