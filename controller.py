import logging
from pathlib import Path
import pandas as pd

from sqlalchemy import create_engine
import hpinv_history_analysis


class YearEndPullController:

    def __init__(
            self,
            output_path: Path,
            pull_start_year: int,
            pull_end_year: int
    ):
        self.output_path = output_path
        self.start_year = pull_start_year
        self.end_year = pull_end_year

        server = 'HC-SQL5'
        database = 'HPINV'
        driver = 'ODBC+Driver+17+for+SQL+Server'
        self.engine = create_engine(
            f"mssql+pyodbc://{server}/{database}?driver={driver}&trusted_connection=yes"
        )

    def pull_year_end(
        self,
        check_for_errors: bool = True
    ) -> pd.DataFrame:
        logging.info("Beginning year end SQL pull.")

        if check_for_errors:
            logging.info("Checking for errors before pulling year end.")
            errors_manager = hpinv_history_analysis.ProgramManager(
                transactions_df=
            )

        with open("queries/define_work_periods.sql", "r") as f:
            work_periods_query = f.read()
        work_periods_df = pd.read_sql(
            work_periods_query,
            con=self.engine,
            params=[self.start_year]
        )

        pull_years = list(range(self.start_year, self.end_year + 1))
        for pull_year in pull_years:
            month_equivalent = (pull_year - self.start_year) * 12
            pull_year_df = work_periods_df[
                (month_equivalent >= work_periods_df['MonthsSinceAdd'])
                & (month_equivalent <= work_periods_df['MonthsSinceDel'])]

        with open("queries/worksite_histories_data.sql", "r") as f:
            worksite_histories_query = f.read()
        worksite_histories_data_df = pd.read_sql(
            worksite_histories_query,
            con=self.engine
        )

        logging.info("Finished year end SQL pull.")
        return work_periods_df


