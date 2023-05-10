import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import matplotlib
import datetime
#matplotlib.use('TkAgg')
# fnord.at:5000/list
table = pd.read_excel('data/active_sensors.xlsx')
table['Last Seen'] = pd.to_datetime(table['Last Seen'])
(table['Last Seen'] > datetime.datetime(2023, 5, 5)).sum()