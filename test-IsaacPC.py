import pandas as pd
from ib_insync import *
import plotly.graph_objects as go
#util.startLoop()  # uncomment this line when in a notebook

ib = IB()
#ib.connect('127.0.0.1', 4002, clientId=1) 
ib.connect(host='127.0.0.1', port=7497, clientId=1, timeout = 5.0) #TWS
empty_df = pd.DataFrame([])
contract = ContFuture('MHI', exchange = 'HKFE', currency = 'HKD')
ib.qualifyContracts(contract)
bars = ib.reqHistoricalData(
    contract, endDateTime='', durationStr='1 W',
    barSizeSetting='5 mins', whatToShow='ADJUSTED_LAST', useRTH=False)
# convert to pandas dataframe (pandas needs to be installed):
df = util.df(bars)[['open','high','low','close','volume']]
#df.to_csv('MHI_test.csv')


#MarketPrice = 20000
#long_limit = MarketPrice
#long_order_parent = LimitOrder('BUY', 2, long_limit)
#
#long_tp = int (MarketPrice +200)
#long_tp_order = Order(action = 'SELL', orderType= 'STP', totalQuantity= 2 , transmit= True)
#long_sl = int(MarketPrice - 100)
#long_sl_order = Order(action = 'SELL', orderType= 'STP', totalQuantity= 2, transmit= True)
#
#longorder_bracket = BracketOrder(long_order_parent, takeProfit= long_tp_order, stopLoss= long_sl_order )
#
#res = ib.placeOrder(contract, longorder_bracket)
#
orders = ib.bracketOrder(action='BUY',quantity=2,limitPrice=20000, takeProfitPrice=22000, stopLossPrice= 19000)

for order in orders:
    res = ib.placeOrder(contract, order)