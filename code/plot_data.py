import pathlib
import geoviews as gv
import hvplot.pandas
import holoviews as hv
from holoviews.operation import decimate
from holoviews.operation import datashader
from holoviews.operation import timeseries
from holoviews.selection import link_selections
import pandas as pd
import pathlib
import os
import numpy as np

from bokeh.plotting import show
import panel as pn
import datetime
hv.extension('bokeh')
pn.extension(comms='vscode')
#pn.extension('notebook')
#from holoviews.operation.datashader import datashade, dynspread
from bokeh.models import HoverTool
from panel.widgets import DateRangeSlider

from bokeh.models import HoverTool, CustomJS
from bokeh.themes import Theme


link = '/home/moritz/Sync/schulwege_data'
data_path = pathlib.Path(link)
interval = '5min'
file_path = data_path.joinpath(f"data_{interval}.xlsx")
df = pd.read_excel(file_path)
# grouped dataframe, therfore ffil required
df["datetime"] = df.datetime.ffill()
# drop names with nan
df = df.loc[~pd.isna(df["name"]),:]


p = df.hvplot.line(x="datetime", y="pm10", 
                   by=['name'], groupby=['ip', 'datetime.month', 'datetime.day'])
p1 = df.hvplot.scatter(x="datetime", y="pm10",
                       by=['name'], groupby=['ip', 'datetime.month', 'datetime.day'],
                       alpha=0.25)
#p.opts(legend_position='right', legend_cols=1)

# l = link_selections(p + p1, selected_color='#ff0000', unselected_alpha=1, unselected_color='#90FF90')
#p.opts(tools=['hover', 'tap', 'box_select'])
# items = []

# from bokeh.models import Legend

# legend1 = Legend(
#     items=items[0:2],
#     location=(0, 15))

# legend2 = Legend(
#     items=items[2:],
#     location=(0, 10))

plot = p*p1
bserve = pn.serve(plot.opts(legend_position='bottom', legend_cols=10,
                          responsive=False, width=1000, height=800) , port=12345)
bserve.stop()