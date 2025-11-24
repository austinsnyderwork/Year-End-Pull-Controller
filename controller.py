import logging
from pathlib import Path
import pandas as pd

import hpinv_sql_pull
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

        self.sql_manager = hpinv_sql_pull.SqlManager()

    def pull_year_end(
        self,
        check_for_errors: bool = True
    ) -> pd.DataFrame:
        logging.info("Beginning year end SQL pull.")

        transactions_df = self.sql_manager.pull(
            query_spec=hpinv_sql_pull.TransactionsSpec(start_year=self.start_year)
        )
        if check_for_errors:
            logging.info("Checking for errors before pulling year end.")
            errors_manager = hpinv_history_analysis.ProgramManager(
                transactions_df=transactions_df
            )

        work_periods_df = self.sql_manager.pull(
            query_spec=hpinv_sql_pull.WorkPeriodsSpec()
        )

        pull_years = list(range(self.start_year, self.end_year + 1))
        for pull_year in pull_years:
            month_equivalent = (pull_year - self.start_year) * 12
            pull_year_df = work_periods_df[
                (month_equivalent >= work_periods_df['MonthsSinceAdd'])
                & (month_equivalent <= work_periods_df['MonthsSinceDel'])]

        worksite_histories_df = self.sql_manager.pull(
            query_spec=hpinv_sql_pull.WorksiteHistorySpec()
        )

        logging.info("Finished year end SQL pull.")
        return work_periods_df


