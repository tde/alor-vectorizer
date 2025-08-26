// auth.js
import axios from 'axios';
import { CONFIG } from './config.js';

let accessToken = null;
let expiresAt = 0;

export async function refreshAccessToken() {
  if (!CONFIG.REFRESH_TOKEN) {
    throw new Error('REFRESH_TOKEN is not set in .env');
  }

  console.log(`url = ${CONFIG.AUTH_URL}`);
  const { data } = await axios.post(CONFIG.AUTH_URL, { token: CONFIG.REFRESH_TOKEN });
  
  accessToken = data.AccessToken;
  const ttl = data.ExpiresIn || 300; // seconds
  expiresAt = Date.now() + Math.max(30, ttl - 30) * 1000; // refresh ~30s before expiry
  
  console.log(`✅ JWT refreshed, ttl≈${ttl}s`);
  return accessToken;
}

export async function getAccessToken() {
  if (!accessToken || Date.now() >= expiresAt) {
    await refreshAccessToken();
  }
  return accessToken;
}