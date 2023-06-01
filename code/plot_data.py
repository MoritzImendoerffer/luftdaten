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

"""
This script does read the aggregated data (e.g. 1 Minute interval) and plots the data for each school.

Requires:

- Prior execution of save_data.py -> generates aggregated data e.g. "data_1min.xlsx"
- The file schulen.xlsx which links the sensor names with dates to school classes and time schedules
- The link to the local folder of cloud.luftdaten.at
- The file table.csv downloaded from "http://fnord.at:5000/list"

Outputs:

Plots per school and day as well as size fraction in the folder "plots"
"""


# Change this link if you are running your script locally
link_to_cloud = '/home/moritz/cloud.luftdaten.at'

# must match f"data_{interval}.xlsx"
interval = '1min'

# do not change folder structures here
internal_path = '2_Projects/Saubere Luft am Schulweg/auswertung/schulwege_data/'
data_path = pathlib.Path(link_to_cloud).joinpath(internal_path)
plot_path = data_path.joinpath('plots')
file_path = data_path.joinpath(f"data_{interval}.xlsx")


df = pd.read_excel(file_path)
# grouped dataframe, therfore ffil required
df["datetime"] = df.datetime.ffill()
# drop names with nan
df = df.loc[~pd.isna(df["name"]),:]

# loads the data from schulen.xlsx and saves as dictionary
table_path = data_path.joinpath("schulen.xlsx")
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

# mapper between name and school
school_mapper = {}
for school in school_dict.keys():
    for name in school_dict[school]["names"]:
        school_mapper[name] = school


for school, item in school_dict.items():
    print(f"Processing: {school}")
    # extract the relevant sensors based on date and name      
    start = datetime.datetime.strptime(item["start"], "%d.%m.%Y")
    end = datetime.datetime.strptime(item["end"], "%d.%m.%Y")
    names = item["names"]
    mask = (df["datetime"] > start) & (df["datetime"] < end) & (df["name"].isin(names))
    slice = df.loc[mask,:]

    # variables to plot
    pm_list = ["pm1", "pm25", "pm10"]

    # create overlay plot for each day and save to the specific data folder (for each school)
    day = copy.copy(start)
    delta = datetime.timedelta(days=1)
    while day < end:
        day_string = day.strftime("%Y_%m_%d")
        print(day)
        mask = (slice["datetime"] >= day) & (slice["datetime"] <= day+delta)
        for pm in pm_list:
            slice_day = slice.loc[mask, :]
            p = slice.hvplot.line(x="datetime", 
                                  y=pm, 
                                  by='name', 
                                  hover_cols=['latitude', 'longitude']).opts(legend_position="bottom", 
                                                                             title=f"{school}, {day_string}", 
                                                                             width=1000, height=800)
            save_path = plot_path.joinpath(school)
            os.makedirs(save_path, exist_ok=True)
            file_name = save_path.joinpath(f'{pm}_{day_string}.html')
            pn.pane.HoloViews(p*hv.HLine(15).opts(color="black",line_dash='dashed',  line_width=2.0)).save(file_name, embed=True, resources=INLINE)

        day += delta