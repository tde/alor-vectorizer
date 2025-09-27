import { OrderBook } from "./OrderBook.js";
import { TradeAggregator } from "./TradeAggregator.js";
import { CONFIG } from "../config.js";
import { Normalizer } from "./Normalizer.js";
import { buildFeatures } from "./FeatureBuilder.js";

export function createPipeline() {
    const ob = new OrderBook();
    const trades = new TradeAggregator();
    const norm = new Normalizer();

    // fixme
    let prevObState = null;
  
    // вызывайте feedLine для каждой входящей JSON-строки
    function feedLine(line) {
        const msg = JSON.parse(line);
      
        // различаем сообщения по наличию полей
        if (msg?.data?.bids && msg?.data?.asks && msg?.data?.bids.length > 0 && msg?.data?.asks.length > 0) {
            // это снимок стакана
            const obState = ob.applySnapshot(msg);
            if (!obState) return;

            // это первый срез стакана, то запомнить и выход
            if (!prevObState) {
                prevObState = obState;
                trades.clear();
                return [];
            }
      
            // агрегируем сделки, случившиеся между предыдущим и текущим 
            //console.log(`prev => ${ob.getSimpleFormat(5, prevObState)}`);
            //console.log(`curr => ${ob.getSimpleFormat(5, obState)}`);
            const aggs = trades.getWindowAggregates(prevObState.ms, obState.ms, prevObState.mid, CONFIG.TICK_SIZE);
            const feat = buildFeatures(obState, aggs, norm);

            // очистить буфер сделок после того как они были агрегированы
            trades.clear();
      
            prevObState = obState;

            //вернуть вектор фич
            return {data: feat, ms: obState.ms, mid: obState.mid};
        }
        //Это сделка
        else if (msg?.data?.price && (msg?.data?.qty || msg?.data?.volume)) {
            trades.push(msg);
            return [];
        }
    }

    return { feedLine };
}