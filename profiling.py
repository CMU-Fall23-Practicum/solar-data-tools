#!/usr/bin/env python

from solardatatools import DataHandler
from pyinstrument.profiler import Profiler
from pyinferno import InfernoRenderer 
from datetime import datetime
import pandas as pd
import os

data_folder_path = './tmp_data/'

def concat_dicts(dicts):
    # Initialize the result dictionary with empty lists 
    result = {key: [] for key in dicts[0].keys()}
    for dict in dicts:
        for key, val in dict.items():
            result[key].append(val)

    return result

prefixes = ['low_time']
for prefix in prefixes:
    profiler = Profiler()
    reports = []
    for filename in os.listdir(data_folder_path):
        file_path = os.path.join(data_folder_path, filename)
        # Check if it's a file and not a sub-directory
        if os.path.isfile(file_path):
            if filename.startswith(prefix) and filename.endswith("csv") and not filename.endswith("profiling.csv"):
                print(f"Profiling: {file_path}")
                df = pd.read_csv(file_path)
                # Only profile running the pipeline and report
                profiler.start()
                dh = DataHandler(df, convert_to_ts=True)
                dh.run_pipeline(power_col='ac_power_01')
                report = dh.report(return_values=True)
                report["file"] = filename
                report["date"] = datetime.now()
                reports.append(report)
                profiler.stop()
    
    if len(reports) > 0:
        reports = concat_dicts(reports)
        df = pd.DataFrame(reports)
        file = f"{data_folder_path}{prefix}_{datetime.now()}_profiling.csv"
        df.to_csv(file)
    output = profiler.output(InfernoRenderer(title=prefix))
    svgpath = f'{data_folder_path}{prefix}_{datetime.now()}flamegraph.svg'
    with open(svgpath, 'w+') as f:
        f.write(output)
