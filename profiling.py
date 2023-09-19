#!/usr/bin/env python

from solardatatools import DataHandler
from pyinstrument.profiler import Profiler
from pyinferno import InfernoRenderer 
import pandas as pd
import os

data_folder_path = './tmp_data/'

prefixes = ['low_time']
for prefix in prefixes:
    profiler = Profiler()
    for filename in os.listdir(data_folder_path):
        file_path = os.path.join(data_folder_path, filename)
    
        # Check if it's a file and not a sub-directory
        if os.path.isfile(file_path):
            if filename.startswith(prefix):
                print(f"Profiling: {file_path}")
                df = pd.read_csv(file_path)
                # Only profile running the pipeline and report
                profiler.start()
                dh = DataHandler(df, convert_to_ts=True)
                dh.run_pipeline(power_col='ac_power_01')
                dh.report()
                profiler.stop()
    
    output = profiler.output(InfernoRenderer(title=prefix))
    svgpath = f'{data_folder_path}{prefix}_flamegraph.svg'
    with open(svgpath, 'w+') as f:
        f.write(output)