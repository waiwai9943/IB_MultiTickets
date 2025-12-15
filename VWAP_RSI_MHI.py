import time
from datetime import datetime, timedelta, time as dt_time
from ib_insync import *
import pandas as pd
import numpy as np
import talib as ta
import logging
from logging.handlers import RotatingFileHandler
import pytz

# Setup logging to file + console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler("strategy.log", maxBytes=5_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)

class VWAPRSIStrategy:
    def __init__(self, ib, symbol='MHI', exchange='HKFE', currency='HKD'):
        self.ib = ib
        self.symbol = symbol
        self.size = 2
        self.contract = ContFuture(symbol, exchange=exchange, currency=currency)
        self.data_df = pd.DataFrame()
        self.current_trade = None

    def connect(self):
        self.ib.connect('127.0.0.1',4002, clientId=1) #4002 gateway;7497 TWS #7496 real
        self.ib.qualifyContracts(self.contract)
        #self.ib.reqMarketDataType(1)  # live data

    def fetch_data(self):
        bars = self.ib.reqHistoricalData(
            self.contract,
            endDateTime='',
            durationStr='2 D',
            barSizeSetting='5 mins',
            whatToShow='TRADES',
            useRTH=False,
            formatDate=1
        )

        df = util.df(bars)
        df.rename(columns={'average': 'vwap'}, inplace=True)
        self.data_df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'vwap']]
        self.calculate_indicators()

    def calculate_indicators(self):
        if len(self.data_df) < 20:
            return
        close = self.data_df['close'].values
        self.data_df['RSI'] = ta.RSI(close, timeperiod=14)
        logging.info('Indicators calculated: VWAP and RSI.')
        logging.info(self.data_df.tail(5))

    def evaluate_trade(self):
        if len(self.data_df) < 20:
            return

        row = self.data_df.iloc[-1]
        price = row['close']
        vwap = row['vwap']
        rsi = row['RSI']

        positions = self.ib.positions()
        in_position = any(p.contract.symbol == self.symbol and p.position != 0 for p in positions)

        if in_position:
            logging.info("Holding position. Skipping signal.")
            return

        if rsi < 30 and price < vwap:
            logging.info(f"LONG signal: Price={price}, VWAP={vwap}, RSI={rsi}")
            self.place_order('BUY', price, vwap)

        elif rsi > 70 and price > vwap:
            logging.info(f"SHORT signal: Price={price}, VWAP={vwap}, RSI={rsi}")
            self.place_order('SELL', price, vwap)
        else:
            logging.info(f"No trade signal: Current Price={price}, VWAP={vwap}, RSI={rsi}")

    def place_order(self, action, entry_price, tp_price):
        qty = self.size
        reverse = 'SELL' if action == 'BUY' else 'BUY'
        self.ib.client.reqIds(-1)  # ensure we get a new order ID
        baseId = self.ib.client.getReqId()

        parent = MarketOrder(action, qty)
        parent.orderId = baseId
        parent.transmit = False

        sl_price = int(entry_price * (0.99 if action == 'BUY' else 1.01))
        limit_price = int(tp_price)

        sl = StopOrder(
            action=reverse,
            totalQuantity=qty,
            stopPrice=sl_price,
            parentId=baseId,
            orderId = baseId + 1,
            tif='GTC',
            transmit=False
        )


        tp = LimitOrder(
            action=reverse,
            totalQuantity=qty,
            lmtPrice=limit_price,
            parentId=baseId,
            orderId = baseId + 2,
            tif = 'GTC',
            transmit=True  # only last child transmits whole chain
        )
#########################################
        #baseId = self.ib.client.getReqId()
        #self.ib.client.reqIds(-1)
        #parent = MarketOrder('BUY', 1)
        #parent.orderId = baseId
        #parent.transmit = False
#
        #sl = StopOrder(
        #    action='SELL',
        #    totalQuantity=1,
        #    stopPrice=24500,
        #    parentId=baseId,
        #    orderId=baseId + 1,
        #    tif='GTC',
        #    transmit=False
        #)
#
        #tp = LimitOrder(
        #    action='SELL',
        #    totalQuantity=1,
        #    lmtPrice=30000,
        #    parentId=baseId,
        #    orderId=baseId + 2,
        #    tif='GTC',
        #    transmit=True
        #)

        for o in [parent, sl, tp]:
            self.ib.placeOrder(self.contract, o)

        print (self.ib.reqOpenOrders())

        logging.info(f"Placed parent + SL + TP: Entry={entry_price}, SL={sl_price}, TP={limit_price}")

    def is_trading_hours(self, now):
    
        day = now.weekday()  # Monday = 0, Sunday = 6
        t = now.time()

        if day >= 5:  # Saturday or Sunday
            return False

        # Day session: 09:15–12:00 and 13:00–16:30
        day_sessions = [
            (dt_time(10, 00), dt_time(12, 0)),
            (dt_time(13, 0), dt_time(16, 30)),
        ]

        # Night session: 17:15–23:59, 00:00–03:00 next day
        night_sessions = [
            (dt_time(17, 15), dt_time(23, 59)),
            (dt_time(0, 0), dt_time(3, 0)),
        ]

        for session in day_sessions + night_sessions:
            if session[0] <= t <= session[1]:
                return True

        return False

    def run(self):
        self.connect()
        logging.info("Strategy started. Waiting for 5-minute bar intervals.")

        while True:
            try:
                hk_tz = pytz.timezone('Asia/Hong_Kong')
                local_now = datetime.now(hk_tz)

                if not self.is_trading_hours(local_now):
                    logging.info("Outside trading hours. Sleeping for 1 minute.")
                    time.sleep(60)
                    continue

                # Calculate next time where minute % 5 == 0 and second = 5

                minute = ((local_now.minute // 5) + 1) * 5
                next_bar = local_now.replace(minute=0, second=5, microsecond=0) + timedelta(minutes=minute)
                if next_bar <= local_now:
                    next_bar += timedelta(minutes=5)
                wait_seconds = (next_bar - local_now).total_seconds()

                logging.info(f"Sleeping {wait_seconds:.0f}s until next bar at {next_bar.strftime('%H:%M:%S')} HK time...")
                time.sleep(wait_seconds)

                self.fetch_data()
                self.evaluate_trade()

            except KeyboardInterrupt:
                logging.info("Strategy stopped manually.")
                break
            except Exception as e:
                logging.exception("Error in strategy loop")
                time.sleep(10)

if __name__ == '__main__':
    ib = IB()
    strategy = VWAPRSIStrategy(ib)
    strategy.run()
