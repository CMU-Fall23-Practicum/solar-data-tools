# from solardatatools import DataHandler
from solardatatools.data_handler import DataHandler
from dask.distributed import Client
import dask.delayed
import pandas as pd
import os


from solardatatools.time_axis_manipulation import (
    make_time_series,
    standardize_time_axis,
    remove_index_timezone,
    get_index_timezone,
)
from solardatatools.matrix_embedding import make_2d
from solardatatools.data_quality import (
    make_density_scores,
    make_linearity_scores,
    make_quality_flags,
)
from solardatatools.data_filling import zero_nighttime, interp_missing
from solardatatools.clear_day_detection import ClearDayDetection
from solardatatools.plotting import plot_2d
from solardatatools.clear_time_labeling import find_clear_times
from solardatatools.solar_noon import avg_sunrise_sunset
from solardatatools.algorithms import (
    CapacityChange,
    TimeShift,
    SunriseSunset,
    ClippingDetection,
)
from pandas.plotting import register_matplotlib_converters
import numpy as np

def run_pipeline_in_parallel(df):
    @dask.delayed
    def preprocess(df, inputs):
        return
    @dask.delayed
    def clipping_check():
        return
    @dask.delayed
    def density_scoring():
        return
    @dask.delayed
    def score_dataset():
        return
    @dask.delayed
    def post_processing():
        return


# @dask.delayed
def run_job(data, data_retrieval_fn):
    """
    Processes a single unit of data using DataHandler.

    Parameters:
    - data: The input data to be processed.
    - data_retrieval_fn: Function to retrieve and format format each data entry.
                         Should return a tuple with the name of the site and a
                         pandas data_frame of the solar data.

    Returns:
    - A dictionary containing the name of the data and the processed report.
    """
    name, data_frame = data_retrieval_fn(data)
    # data_handler = DataHandler(data_frame, convert_to_ts=True,)
    # data_handler.run_pipeline(power_col='ac_power_01', solver_convex="OSQP")
    # data_handler.run_pipeline(power_col='ac_power_01', )
    data = run_pipeline_in_parallel(data_frame)
    report = {}
    report["name"] = name
    report["data"] = data
    return report


def local_csv_to_df(file):
    """
    Converts a local CSV file into a pandas DataFrame.

    Parameters:
    - file: Path to the CSV file.

    Returns:
    - A tuple of the file name and its corresponding DataFrame.
    """
    df = pd.read_csv(file)
    name = os.path.basename(file)
    return name, df


class ParallelDataHandler:
    """
    A class to handle and process multiple data entries in parallel.
    """
    def __init__(self, data_list, data_retrieval_fn):
        """
        Initializes the ParallelDataHandler with a list of data and a retrieval function.

        Parameters:
        - data_list: A list containing data entries to be processed.
    - data_retrieval_fn: Function to retrieve and format format each data entry.
                         Should return a tuple with the name of the site and a
                         pandas data_frame of the solar data.
        """

        self.data_retrieval_fn = data_retrieval_fn
        self.data_list = data_list

    def run_pipelines_in_parallel(self):
        """
        Executes the data processing pipelines in parallel on the provided data list.

        Returns:
        - A list of reports generated from processing each data entry.
        """
        reports = []
        for data in self.data_list:
            reports.append(run_job(data, self.data_retrieval_fn))
        computed_reports = dask.compute(*reports)
        return computed_reports


if __name__ == "__main__":
    # Set up a Dask client for parallel computation
    client = Client()
    files = ['/home/jose/Schoolwork/Practicum/solar-data-tools/tmp_data/low_time_0022F200152D.csv',]
    # files = ['/home/jose/Schoolwork/Practicum/solar-data-tools/tmp_data/high_time_ZT155185000441C0083.csv','/home/jose/Schoolwork/Practicum/solar-data-tools/tmp_data/low_time_0022F200152D.csv','/home/jose/Schoolwork/Practicum/solar-data-tools/tmp_data/high_time_ZT161685000441C0141.csv',]

    # Initialize the ParallelDataHandler with the list of files and the CSV-to-DataFrame conversion function
    local_hander = ParallelDataHandler(files, local_csv_to_df)

    reports = local_hander.run_pipelines_in_parallel()
    print(reports)
