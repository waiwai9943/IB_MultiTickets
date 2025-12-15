import time
import pytz
from datetime import datetime, timedelta
from ib_insync import *
import pandas as pd
import numpy as np
import talib as ta
import logging
from logging.handlers import RotatingFileHandler

# Setup logging to file + console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler("strategy_VWAP_RSI_SPY.log", maxBytes=5_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)

class VWAPRSIStrategy:
    def __init__(self, ib, symbol='NQ', exchange='CME', currency='USD'):
        self.ib = ib
        self.symbol = symbol
        self.size = 2  # Default order size
        # For SPY, which is an ETF (Stock), use the Stock contract type.
        # 'ARCA' is a common exchange for SPY. 'SMART' can also be used for automatic routing.
        self.contract = ContFuture(symbol, exchange=exchange, currency=currency)
        self.data_df = pd.DataFrame()
        self.interval = 3 #mins
        self.current_trade = None # To track current open trade, if any

    def connect(self):
        """
        Connects to the Interactive Brokers TWS/Gateway.
        """
        logging.info("Attempting to connect to IB TWS/Gateway...")
        self.ib.connect('127.0.0.1', 4002, clientId=1) #7496 TWS, 4002 Gateway
        logging.info("Connected to IB TWS/Gateway.")
        # Qualify the contract to ensure all details are correct and available
        self.ib.qualifyContracts(self.contract)
        # Request live data (1 for live, 2 for frozen, 3 for delayed, 4 for delayed-frozen)
        self.ib.reqMarketDataType(1)
        logging.info(f"Qualified contract: {self.contract.localSymbol} on {self.contract.exchange}")

    def fetch_data(self):
        """
        Fetches historical 5-minute bar data for the specified contract.
        """
        logging.info("Fetching historical data...")
        bars = self.ib.reqHistoricalData(
            self.contract,
            endDateTime='',  # Empty string means current time
            durationStr='1 D',  # Fetch 2 days of data
            barSizeSetting='%s mins'%self.interval, # 5-minute bars
            whatToShow='TRADES', # Show trade data
            useRTH=False, # Use regular trading hours only (True) or all available data (False)
                          # Keeping False for consistency with original, but True is common for stocks.
            formatDate=1 # Format date as YYYYMMDD hh:mm:ss
        )

        # Convert historical bars to a pandas DataFrame
        df = util.df(bars)
        # Rename 'average' column to 'vwap' for consistency with the strategy logic
        df.rename(columns={'average': 'vwap'}, inplace=True)
        # Select relevant columns
        self.data_df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'vwap']]
        logging.info(f"Fetched {len(self.data_df)} bars of data.")
        # Calculate indicators after fetching new data
        self.calculate_indicators()

    def calculate_indicators(self):
        """
        Calculates the RSI indicator based on the 'close' prices in the data_df.
        """
        # Ensure there's enough data to calculate RSI (default period is 14)
        if len(self.data_df) < 20: # A bit more than 14 to be safe
            logging.warning("Not enough data to calculate indicators.")
            return

        close_prices = self.data_df['close'].values
        # Calculate Relative Strength Index (RSI) with a period of 14
        self.data_df['RSI'] = ta.RSI(close_prices, timeperiod=14)
        logging.info("Indicators (RSI) calculated.")

    def evaluate_trade(self):
        """
        Evaluates the current market conditions based on VWAP and RSI to generate trade signals.
        Places a trade if a signal is identified and no position is currently held.
        """
        # Ensure there's enough data for evaluation, especially for RSI
        if len(self.data_df) < 20:
            logging.warning("Not enough data to evaluate trade.")
            return

        # Get the latest bar's data
        row = self.data_df.iloc[-1]
        price = row['close']
        vwap = row['vwap']
        rsi = row['RSI']

        logging.info(f"Current Data: Price={price}, VWAP={vwap}, RSI={rsi:.2f}")

        # Check current positions to avoid multiple entries
        positions = self.ib.positions()
        in_position = any(p.contract.symbol == self.symbol and p.position != 0 for p in positions)

        if in_position:
            logging.info("Currently holding a position. Skipping new signal evaluation.")
            return

        # Long signal: RSI below 30 (oversold) and Price below VWAP
        if rsi < 30 and price < vwap:
            logging.info(f"LONG signal detected: Price={price}, VWAP={vwap}, RSI={rsi:.2f}. Attempting to place BUY order.")
            self.place_order('BUY', price, vwap) # Use VWAP as a potential take profit target
        # Short signal: RSI above 70 (overbought) and Price above VWAP
        elif rsi > 70 and price > vwap:
            logging.info(f"SHORT signal detected: Price={price}, VWAP={vwap}, RSI={rsi:.2f}. Attempting to place SELL order.")
            self.place_order('SELL', price, vwap) # Use VWAP as a potential take profit target
        else:
            logging.info("No trade signal currently. Waiting for conditions.")

    def place_order(self, action, entry_price, tp_price):
        qty = self.size
        reverse = 'SELL' if action == 'BUY' else 'BUY'
        self.ib.client.reqIds(-1)  # ensure we get a new order ID
        baseId = self.ib.client.getReqId()

        parent = LimitOrder(action, qty, entry_price)
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

        for o in [parent, sl, tp]:
            self.ib.placeOrder(self.contract, o)

        print (self.ib.reqOpenOrders())

        logging.info(f"Placed parent + SL + TP: Entry={entry_price}, SL={sl_price}, TP={limit_price}")

    def call_position(self):        
        positions = self.ib.positions()
        print('Current Positions:', positions)
        logging.info('Current Positions:', positions)

    def run(self):
        """
        Runs the main strategy loop, connecting to IB and periodically fetching data
        and evaluating trade opportunities. Only runs during US regular market hours.
        """
        self.connect()
        logging.info("Strategy started. Waiting for 5-minute bar intervals to align.")

        # US/Eastern timezone for NYSE/NASDAQ
        eastern = pytz.timezone('US/Eastern')

        while True:
            try:
                now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
                now_est = now_utc.astimezone(eastern)

                # US market regular trading hours
                market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
                market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)

                if not (market_open <= now_est <= market_close):
                    logging.info(f"Outside trading hours ({now_est.strftime('%H:%M:%S %Z')}). Sleeping until market open.")

                    if now_est >= market_close:
                        # If after close, sleep until next day open
                        next_open = market_open + timedelta(days=1)
                    else:
                        # Before open today
                        next_open = market_open

                    wait_seconds = (next_open - now_est).total_seconds()
                    logging.info(f"Sleeping {wait_seconds/60:.1f} minutes until next market open at {next_open.strftime('%H:%M:%S %Z')}.")
                    time.sleep(wait_seconds)
                    continue

                # Calculate next 5-min bar mark
                now = datetime.now()
                current_minute = now.minute
                next_interval_minute = ((current_minute // self.interval) + 1) * self.interval

                if next_interval_minute == 60:
                    next_bar = now.replace(minute=0, second=self.interval, microsecond=0) + timedelta(hours=1)
                else:
                    next_bar = now.replace(minute=next_interval_minute, second=self.interval, microsecond=0)

                if next_bar <= now:
                    next_bar += timedelta(minutes=self.interval)

                wait_seconds = (next_bar - now).total_seconds()
                logging.info(f"Sleeping {wait_seconds:.0f}s until next bar at {next_bar.strftime('%H:%M:%S')}...")
                time.sleep(wait_seconds)

                # Fetch new data and evaluate trade
                self.fetch_data()
                self.evaluate_trade()
                self.call_position()

            except KeyboardInterrupt:
                logging.info("Strategy stopped manually by user (KeyboardInterrupt).")
                break
            except Exception as e:
                logging.exception("An error occurred in the main strategy loop.")
                time.sleep(10)


if __name__ == '__main__':
    # Create an instance of IB (Interactive Brokers)
    ib = IB()
    # Create an instance of the strategy, passing the IB object
    strategy = VWAPRSIStrategy(ib)
    # Run the strategy
    strategy.run()

