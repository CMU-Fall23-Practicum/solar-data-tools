from solardatatools import DataHandler
from dask.distributed import Client
import dask.delayed
import pandas as pd
import os


@dask.delayed
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
    data_handler = DataHandler(data_frame, convert_to_ts=True,)
    # data_handler.run_pipeline(power_col='ac_power_01', solver_convex="OSQP")
    data_handler.run_pipeline(power_col='ac_power_01', )
    report = {}
    report["name"] = name
    report["data"] = data_handler.report(verbose=True, return_values=True)
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
