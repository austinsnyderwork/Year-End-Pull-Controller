from pathlib import Path

import controller

c = controller.YearEndPullController(
    output_path=Path("C:/Users/austisnyder/TechnicalWork/InputOutputRepo/Year-End/2025"),
    pull_start_year=1977,
    pull_end_year=2025
)
pull_df = c.pull_year_end()
x=0
