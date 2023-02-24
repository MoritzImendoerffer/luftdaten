"""
Convert the csv data per folder to parquet files grouped per sensor_id
"""

import logging
import os
import subprocess
import numpy as np
import pandas as pd

def unzip(file_path, save_path):
    cmd = "unzip" + ' ' + file_path + ' -d' + ' ' + save_path
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True
    )
    std_out, std_err = process.communicate()
