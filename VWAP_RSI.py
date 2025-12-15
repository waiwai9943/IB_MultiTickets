import asyncio
import logging
import pandas as pd
import numpy as np
from ib_insync import IB, Stock, Order, Trade, util
import talib as ta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VWAPPullbackIB:
    def __init__(self, ib: IB, symbol='SPY', exchange='SMART', currency='USD', bar_size='5 mins', history_duration='5 D'):
        self.ib = ib
        self.symbol = symbol
        self.contract = Stock(symbol, exchange, currency)
        self.bar_size = bar_size
        self.history_duration = history_duration
        self.data_df = pd.DataFrame()
        self.position = None
        self.vwap_series = pd.Series(dtype=float)
        self.rsi_series = pd.Series(dtype=float)
        self.current_trade = None

    async def init_data(self):
        await self.ib.qualifyContractsAsync(self.contract)
        bars = await self.ib.reqHistoricalDataAsync(
            self.contract, '', self.history_duration, self.bar_size,
            'TRADES', False, 1, useRTH=True
        )
        if not bars:
            logging.warning("No historical bars received.")
            return

        df = util.df(bars)
        df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'average']]
        df.rename(columns={'average': 'vwap'}, inplace=True)
        self.data_df = df
        self.calculate_indicators()

    def calculate_indicators(self):
        if len(self.data_df) < 20:
            return
        close = self.data_df['close'].values
        self.vwap_series = pd.Series(self.data_df['vwap'].values)
        self.rsi_series = pd.Series(ta.RSI(close, timeperiod=14))

    async def on_bar_update(self, bars, has_gaps):
        for bar in bars:
            new_row = pd.DataFrame([{
                'date': bar.time,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'vwap': bar.average
            }])
            self.data_df = pd.concat([self.data_df, new_row], ignore_index=True)
            self.data_df = self.data_df.tail(500)
            self.calculate_indicators()
            await self.next_logic()

    async def next_logic(self):
        if len(self.rsi_series) < 15:
            return

        price = self.data_df['close'].iloc[-1]
        vwap = self.vwap_series.iloc[-1]
        rsi = self.rsi_series.iloc[-1]

        # Check current positions
        self.position = await self.ib.reqPositionsAsync()
        open_position = any(p.contract.symbol == self.symbol and p.position != 0 for p in self.position)

        if open_position:
            logging.info(f"Already in a position for {self.symbol}.")
            return

        if price < vwap and rsi < 30:
            sl = round(price * 0.99, 2)
            tp = round(vwap, 2)
            logging.info(f"LONG setup - Price: {price}, VWAP: {vwap}, RSI: {rsi}")
            await self.place_trade('BUY', 10, sl, tp)

        elif price > vwap and rsi > 70:
            sl = round(price * 1.01, 2)
            tp = round(vwap, 2)
            logging.info(f"SHORT setup - Price: {price}, VWAP: {vwap}, RSI: {rsi}")
            await self.place_trade('SELL', 10, sl, tp)

    async def place_trade(self, action, qty, sl_price, tp_price):
        order = Order(
            action=action,
            totalQuantity=qty,
            orderType='MKT',
            tif='DAY',
            outsideRth=False
        )
        trade = self.ib.placeOrder(self.contract, order)
        self.current_trade = trade
        trade.filledEvent += lambda t: asyncio.ensure_future(self.attach_sl_tp(t, sl_price, tp_price, action))
        logging.info(f"{action} order placed for {qty} shares.")

    async def attach_sl_tp(self, trade: Trade, sl_price, tp_price, action):
        if trade.orderStatus.status != 'Filled':
            return
        filled_qty = trade.filled()
        parent_order_id = trade.order.orderId
        reverse_action = 'SELL' if action == 'BUY' else 'BUY'

        # Stop Loss
        stop_order = Order(
            action=reverse_action,
            totalQuantity=filled_qty,
            orderType='STP',
            auxPrice=sl_price,
            parentId=parent_order_id,
            tif='GTC',
            outsideRth=False
        )
        self.ib.placeOrder(self.contract, stop_order)
        logging.info(f"Stop loss order placed at {sl_price}")

        # Take Profit
        tp_order = Order(
            action=reverse_action,
            totalQuantity=filled_qty,
            orderType='LMT',
            lmtPrice=tp_price,
            parentId=parent_order_id,
            tif='GTC',
            outsideRth=False
        )
        self.ib.placeOrder(self.contract, tp_order)
        logging.info(f"Take profit order placed at {tp_price}")

    async def run(self):
        logging.info("Connecting to IB...")
        await self.ib.connectAsync('127.0.0.1', 4002, clientId=1)
        self.ib.reqMarketDataType(1)
        await self.init_data()

        self.ib.reqRealTimeBars(self.contract, 5, 'TRADES', False, self.on_bar_update)
        logging.info("Subscribed to real-time bars.")
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logging.info("Stopped.")
        finally:
            self.ib.disconnect()
            logging.info("Disconnected from IB.")

async def main():
    ib = IB()
    strategy = VWAPPullbackIB(ib)
    await strategy.run()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("User stopped the strategy.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
