import time
import pytz
from datetime import datetime, timedelta
from ib_insync import *
import pandas as pd
import numpy as np
import talib as ta
import logging
from logging.handlers import RotatingFileHandler
import math

# Setup logging to file + console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler("strategy_VWAP_RSI_MULTI.log", maxBytes=5_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)

class MultiSymbolVWAPRSIStrategy:
    def __init__(self, ib, symbols, exchange='SMART', currency='USD'):
        """
        Initializes the Multi-Symbol VWAP RSI Strategy.
        Args:
            ib: An instance of ib_insync.IB.
            symbols (list): A list of trading symbols (e.g., ['SPY', 'QQQ', 'MSFT']).
            exchange (str): The exchange (default 'SMART' for automatic routing).
            currency (str): The currency (default 'USD').
        """
        self.ib = ib
        self.symbols = symbols
        self.currency = currency # Store currency for account summary filtering
        
        # Create contract objects for each symbol
        self.contracts = {}
        for sym in self.symbols:
            self.contracts[sym] = Stock(sym, exchange, currency)
            
        # Storage for dataframes for each symbol
        self.data_dfs = {sym: pd.DataFrame() for sym in self.symbols}
        
    def connect(self):
        """
        Connects to IB and sets up Account Summary subscriptions.
        """
        if self.ib.isConnected():
            return

        logging.info("Attempting to connect to IB TWS/Gateway...")
        try:
            self.ib.connect('127.0.0.1', 4002, clientId=1) 
            logging.info("Connected to IB TWS/Gateway.")
        except Exception as e:
            logging.error(f"Connection failed: {e}")
            return
        
        # --- FIX: Use reqAccountSummary with correct keywords ---
        logging.info("Requesting Account Summary...")
        # Note: The correct keyword arguments are 'groupName' and 'tags'
        self.ib.reqAccountSummary(groupName="All", tags="NetLiquidation,TotalCashValue")
        
        # WAIT LOOP: Ensure we actually have data before proceeding
        logging.info("Waiting for account data to populate...")
        start_wait = time.time()
        while time.time() - start_wait < 10:
            if self.ib.accountSummary():
                logging.info("Account data received.")
                break
            self.ib.sleep(0.5)
        else:
            logging.warning("Timed out waiting for Account Data. Using defaults if needed.")

        # Qualify Contracts
        logging.info(f"Qualifying contracts for: {self.symbols}")
        for sym in self.symbols:
            try:
                self.ib.qualifyContracts(self.contracts[sym])
                logging.info(f"Qualified: {self.contracts[sym].localSymbol}")
            except Exception as e:
                logging.error(f"Could not qualify {sym}: {e}")

        self.ib.reqMarketDataType(1) 

    def fetch_data(self, symbol):
        """
        Fetches historical 5-minute bar data for a specific symbol.
        """
        contract = self.contracts[symbol]
        
        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime='',  # Current time
            durationStr='2 D',  # 2 Days lookback
            barSizeSetting='5 mins',
            whatToShow='TRADES',
            useRTH=False,
            formatDate=1
        )

        if not bars:
            logging.warning(f"No data returned for {symbol}")
            return

        df = util.df(bars)
        # Rename 'average' to 'vwap'
        if 'average' in df.columns:
            df.rename(columns={'average': 'vwap'}, inplace=True)
            
        self.data_dfs[symbol] = df[['date', 'open', 'high', 'low', 'close', 'volume', 'vwap']]
        
        # Calculate indicators immediately after fetching
        self.calculate_indicators(symbol)

    def calculate_indicators(self, symbol):
        """
        Calculates RSI for the specific symbol's dataframe.
        """
        df = self.data_dfs[symbol]
        if len(df) < 20:
            return

        close_prices = df['close'].values
        df['RSI'] = ta.RSI(close_prices, timeperiod=14)
        self.data_dfs[symbol] = df  # Update dict

    def evaluate_trade(self, symbol):
        """
        Evaluates trade logic for a specific symbol.
        Returns True if a trade was executed, False otherwise.
        """
        df = self.data_dfs[symbol]
        if len(df) < 20:
            return False

        row = df.iloc[-1]
        price = row['close']
        vwap = row['vwap']
        rsi = row['RSI']
        
        # --- SIGNAL LOGIC ---
        # Long: RSI < 30 (Oversold) AND Price < VWAP
        if rsi < 30 and price < vwap:
            logging.info(f"🚀 MATCH FOUND [{symbol}]: LONG Signal (RSI {rsi:.2f} < 30, Price {price} < VWAP {vwap:.2f})")
            self.place_order(symbol, 'BUY', price, vwap)
            return True # Signal found and acted upon

        # Short: RSI > 70 (Overbought) AND Price > VWAP
        elif rsi > 70 and price > vwap:
            logging.info(f"🚀 MATCH FOUND [{symbol}]: SHORT Signal (RSI {rsi:.2f} > 70, Price {price} > VWAP {vwap:.2f})")
            self.place_order(symbol, 'SELL', price, vwap)
            return True # Signal found and acted upon

        return False

    def get_account_balances(self):
        """
        Retrieves Equity and Cash using accountSummary().
        """
        usd_cash = 0.0

        # Iterate through the accountSummary list
        for item in self.ib.accountSummary():
            if item.tag == 'NetLiquidationByCurrency' and item.currency == 'USD':
                    try: 
                        usd_cash = float(item.value)
                    except: pass    
        return usd_cash

    def place_order(self, symbol, action, entry_price, tp_target_price):
        """
        Places a Bracket Order with Dynamic Position Sizing.
        Logic:
        1. Size is determined by available Cash (Invest fully).
        2. Stop Loss is calculated to cap risk at 2% of Total Equity.
        """
        contract = self.contracts[symbol]
        reverse_action = 'SELL' if action == 'BUY' else 'BUY'
        
        # --- 1. GET ACCOUNT DATA (Using accountSummary) ---
        usd_cash = self.get_account_balances()
        if usd_cash == 0: 
            logging.error("Could not fetch Account Equity from accountSummary. Defaulting to safe small size.")
            equity = 50000 # Safety fallback
            cash = 50000

        # --- 2. CALCULATE SIZE BASED ON CASH (Buying Power) ---
        # "calculate the appropriate size by looking at the current available cash"
        # We assume we want to utilize available cash for the trade.
        # We use 98% of cash to leave room for commissions and slippage.
        usable_cash = usd_cash * 0.98
        
        if entry_price == 0: 
            logging.error("Entry price is zero. Cannot calculate size.")
            return

        final_qty = math.floor(usable_cash / entry_price)
        
        if final_qty < 1:
            logging.warning(f"Calculated quantity is 0 (Cash: {cash}, Price: {entry_price}). Aborting trade.")
            return

        # --- 3. CALCULATE STOP LOSS PRICE TO RISK 2% OF EQUITY ---
        # "stop loss should be calculated accordingly" to risk 2% of equity
        risk_budget_dollars = equity * 0.02
        
        # Formula: Total Risk = Quantity * (Entry - SL)
        # Therefore: (Entry - SL) = Total Risk / Quantity
        risk_per_share = risk_budget_dollars / final_qty
        
        if action == 'BUY':
            sl_price = round(entry_price - risk_per_share, 2)
        else: # SELL
            sl_price = round(entry_price + risk_per_share, 2)

        # --- LOGGING THE CALCULATION ---
        logging.info(f"--- SIZING CALCULATION for {symbol} ---")
        logging.info(f"Equity: ${equity:,.2f} | Cash: ${cash:,.2f}")
        logging.info(f"Size (Cash-Based): {final_qty} shares (Using 98% of ${cash:,.2f})")
        logging.info(f"Risk Budget (2%): ${risk_budget_dollars:,.2f}")
        logging.info(f"Implied Stop Dist: ${risk_per_share:.2f}")
        logging.info(f"Entry: {entry_price} | SL Price: {sl_price}")
        logging.info("---------------------------------------")

        self.ib.client.reqIds(-1)
        baseId = self.ib.client.getReqId()

        # 1. Parent Market Order
        parent = MarketOrder(action, final_qty)
        parent.orderId = baseId
        parent.transmit = False

        # 2. Stop Loss Order
        sl = StopOrder(
            action=reverse_action,
            totalQuantity=final_qty,
            stopPrice=sl_price,
            parentId=baseId,
            orderId=baseId + 1,
            tif='GTC',
            transmit=False
        )

        # 3. Take Profit Order
        tp_price = round(tp_target_price, 2)
        tp = LimitOrder(
            action=reverse_action,
            totalQuantity=final_qty,
            lmtPrice=tp_price,
            parentId=baseId,
            orderId=baseId + 2,
            tif='GTC',
            transmit=True # Transmit the whole bracket
        )

        orders = [parent, sl, tp]
        for o in orders:
            self.ib.placeOrder(contract, o)

        logging.info(f"✅ Trade Executed for {symbol}: {action} {final_qty} shares @ ~{entry_price}. SL={sl_price}, TP={tp_price}")

    def run(self):
        """
        Main Loop: Monitors market hours, fetches data for list, executes first hit.
        """
        self.connect()
        logging.info("Strategy started. Monitoring symbols: " + str(self.symbols))
        
        eastern = pytz.timezone('US/Eastern')

        while True:
            try:
                # 1. Time & Market Hours Check
                now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
                now_est = now_utc.astimezone(eastern)
                
                market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
                market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)

                # Simple check: sleep if outside hours
                if not (market_open <= now_est <= market_close):
                    logging.info(f"Market Closed ({now_est.strftime('%H:%M')}). Waiting...")
                    self.ib.sleep(60) # Sleep 1 min
                    continue

                # 2. Check Global Position Limit & Print/Log Positions
                positions = self.ib.positions()
                
                print("\n====== POSITION CHECK ======")
                if positions:
                    logging.info("--- Current Holdings ---")
                    for p in positions:
                        if p.position != 0:
                            pos_details = f"SYMBOL: {p.contract.symbol} | SIZE: {p.position} | AVG COST: {p.avgCost}"
                            print(pos_details)
                            logging.info(pos_details)
                else:
                    msg = "Current Holdings: FLAT (No positions)"
                    print(msg)
                    logging.info(msg)
                print("============================\n")

                # Check if we hold any quantity of the symbols we are monitoring
                active_symbols = [p.contract.symbol for p in positions if p.position != 0]
                
                if len(active_symbols) > 0:
                    logging.info(f"⚠️ Active Position detected in {active_symbols}. Pausing scan until closed.")
                    self.ib.sleep(30) # Check again in 30 seconds
                    continue

                # 3. Scan Symbols
                logging.info(f"Scanning {self.symbols} for opportunities...")
                
                for sym in self.symbols:
                    # Fetch and Evaluate
                    self.fetch_data(sym)
                    trade_occurred = self.evaluate_trade(sym)
                    
                    if trade_occurred:
                        logging.info(f"🛑 Stopping scan cycle. Trade initiated on {sym}.")
                        break # "whichever the first hit... execute" -> stop checking others
                
                # 4. Wait for next bar
                now = datetime.now()
                next_min = ((now.minute // 5) + 1) * 5
                if next_min == 60:
                    next_time = (now + timedelta(hours=1)).replace(minute=0, second=5)
                else:
                    next_time = now.replace(minute=next_min, second=5)
                
                wait_seconds = (next_time - now).total_seconds()
                if wait_seconds < 0: wait_seconds = 300 # Safety fallback
                
                logging.info(f"Scan complete. Sleeping {wait_seconds:.0f}s until {next_time.strftime('%H:%M:%S')}")
                
                self.ib.sleep(wait_seconds)

            except KeyboardInterrupt:
                logging.info("Manual Stop.")
                break
            except Exception as e:
                logging.exception("Error in main loop")
                self.ib.sleep(10)

if __name__ == '__main__':
    ib = IB()
    # Define the list of stocks to monitor
    stock_list = ['SPY', 'QQQ', 'MSFT', 'TSLA']
    
    strategy = MultiSymbolVWAPRSIStrategy(ib, stock_list)
    strategy.run()