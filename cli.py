import os
import logging
import argparse

from dags.sample_etl import sample_etl

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run the sample ETL pipeline")
    parser.add_argument(
        "--file-path",
        type=str,
        help="The path to the file to be used in the ETL pipeline",
        required=True,
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    """Read in file and run airflow task.

    :param args:
    :return:
    """
    # log some basic info about the args
    logger.info(f"Running ETL pipeline with file: {args.file_path}")

    # check contents of file
    if not os.path.exists(args.file_path):
        raise ValueError(f"File does not exist at path {args.file_path}")

    # run the airflow task
    with sample_etl(file_path=args.file_path) as dag:
        logger.info(f"ETL pipeline starting with file: {args.file_path}")
        dag.run()
        logger.info("ETL pipeline complete")


if __name__ == "__main__":
    main(parse_args())
