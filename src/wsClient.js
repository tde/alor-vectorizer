import WebSocket from 'ws';
import { v4 as uuidv4 } from 'uuid';
import { CONFIG } from './config.js';
import { getAccessToken, refreshAccessToken } from './auth.js';

export class AlorWS {
  constructor() {
    this.ws = null;
    this.onMessage = null;
    this.connected = false;
  }

  async connect(onMessage) {
    this.onMessage = onMessage;
    this.ws = new WebSocket(CONFIG.WS_URL);

    this.ws.on('open', async () => {
      this.connected = true;
      console.log('🔌 WebSocket connected');
      await this.subscribeAll();
    });

    this.ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        this.onMessage?.(msg);
      } catch {
        // ignore
      }
    });

    this.ws.on('close', () => {
      this.connected = false;
      console.log('❌ WS closed, reconnecting in 2s…');
      setTimeout(() => this.connect(this.onMessage), 2000);
    });

    this.ws.on('error', (err) => {
      console.error('WS error:', err.message);
    });
  }

  send(obj) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(obj));
    }
  }

  async subscribeAll() {
    const token = await getAccessToken();
    // OrderBook
    this.send({
      opcode: 'OrderBookGetAndSubscribe',
      code: CONFIG.SYMBOL,
      depth: CONFIG.DEPTH,
      exchange: CONFIG.EXCHANGE,
      format: 'Simple',
      frequency: CONFIG.FREQUENCY,
      guid: `ob-${uuidv4()}`,
      token,
    });
    // Trades (поток сделок)
    this.send({
      opcode: 'AllTradesGetAndSubscribe',
      code: CONFIG.SYMBOL,
      exchange: CONFIG.EXCHANGE,
      format: 'Simple',
      guid: `tr-${uuidv4()}`,
      token,
    });
  }

  async renew() {
    if (!this.connected) return;
    await refreshAccessToken();
    await this.subscribeAll(); // переподписка с новым токеном
  }
}
