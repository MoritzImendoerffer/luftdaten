import pathlib
import hvplot.pandas
import holoviews as hv
from bokeh.resources import INLINE
import pandas as pd
import pathlib
import os
import numpy as np
import panel as pn
import datetime
hv.extension('bokeh')
pn.extension(comms='vscode')

import copy



link = '/home/moritz/Sync/schulwege_data'
data_path = pathlib.Path(link)
interval = '1min'
file_path = data_path.joinpath(f"data_{interval}.xlsx")
df = pd.read_excel(file_path)
# grouped dataframe, therfore ffil required
df["datetime"] = df.datetime.ffill()
# drop names with nan
df = df.loc[~pd.isna(df["name"]),:]

table_path = data_path.joinpath(f"schulen.xlsx")
efile = pd.ExcelFile(table_path)
sheets = efile.sheet_names
school_dict = {}
for sheet in sheets:
    temp_df = efile.parse(sheet_name=sheet)
    school = temp_df["Schule"][0]
    school_dict[school] = {}
    school_dict[school]["start"] = temp_df["Start"][0]
    school_dict[school]["end"] = temp_df["Ende"][0]
    school_dict[school]["names"] = temp_df["Nummern"]

school_mapper = {}
for school in school_dict.keys():
    for name in school_dict[school]["names"]:
        school_mapper[name] = school

item = school_dict['VS Längenfeld']
start = datetime.datetime.strptime(item["start"], "%d.%m.%Y")
end = datetime.datetime.strptime(item["end"], "%d.%m.%Y")
names = item["names"]
mask = (df["datetime"] > start) & (df["datetime"] < end) & (df["name"].isin(names))
df.loc[mask,:]

save_path = pathlib.Path("./plots")
for school, item in school_dict.items():
    print(school)
    start = datetime.datetime.strptime(item["start"], "%d.%m.%Y")
    end = datetime.datetime.strptime(item["end"], "%d.%m.%Y")
    names = item["names"]
    mask = (df["datetime"] > start) & (df["datetime"] < end) & (df["name"].isin(names))
    slice = df.loc[mask,:]

    pm_list = ["pm1", "pm25", "pm10"]

    day = copy.copy(start)
    delta = datetime.timedelta(days=1)
    while day < end:
        day_string = day.strftime("%Y_%m_%d")
        print(day)
        for pm in pm_list:
            slice_day = df.loc[mask, :]
            p = slice.hvplot.line(x="datetime", y=pm, by='name', hover_cols=['latitude', 'longitude']).opts(legend_position="bottom", title=f"{school}, {day_string}", width=1000, height=800)
            save_path = pathlib.Path(f"/home/moritz/Sync/schulwege_data/plots_new/{school}")
            os.makedirs(save_path, exist_ok=True)
            file_name = save_path.joinpath(f'{pm}_{day_string}.html')
            pn.pane.HoloViews(p*hv.HLine(15).opts(color="black",line_dash='dashed',  line_width=2.0)).save(file_name, embed=True, resources=INLINE)

        day += delta