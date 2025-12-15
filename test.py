import pandas as pd
from ib_insync import *
import plotly.graph_objects as go
#util.startLoop()  # uncomment this line when in a notebook

ib = IB()
ib.connect('127.0.0.1', 4002, clientId=1)
empty_df = pd.DataFrame([])
contract = ContFuture('MHI', exchange = 'HKFE', currency = 'HKD')
bars = ib.reqHistoricalData(
    contract, endDateTime='', durationStr='1 W',
    barSizeSetting='5 mins', whatToShow='ADJUSTED_LAST', useRTH=False)
# convert to pandas dataframe (pandas needs to be installed):
df = util.df(bars)[['open','high','low','close','volume']]
df.to_csv('MHI_test.csv')