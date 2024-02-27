from __future__ import annotations

import os
import logging

import pendulum
import pandas as pd

from typing import Dict
from airflow.decorators import dag, task


INCOME_CSV = "dags/data/income.csv"

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def sample_etl(file_path: str = INCOME_CSV):
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    @task()
    def extract(file_path: str) -> Dict:
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        # check contents of file
        if not os.path.exists(file_path):
            raise ValueError(f"File does not exist at path {file_path}")

        # load data into a dataframe
        df = pd.read_csv(file_path)

        # show some basic info about the dataframe
        logger.info(f"DataFrame: {df.info()}")

        # return list of dictionaries
        income_data = df.to_dict(orient="records")
        return income_data

    @task(multiple_outputs=True)
    def transform(income_data: Dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        if len(income_data) == 0:
            raise ValueError("No data present")

        # TODO: Do some basic processing

        return income_data

    @task()
    def load(income_data: Dict):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        print(f"Total number of values in list {len(income_data)}")

    income_data = extract(file_path=file_path)
    income_data = transform(income_data=income_data)
    load(income_data=income_data)
