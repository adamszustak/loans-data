import glob
import random
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Iterator

from utils import (CUSTOMERS_FILENAME, DATES_FILENAME, FOLDER_NAME_FOR_FILES,
                   Reader, Writer)

HEADERS = [
    "customer_id",
    "first_name",
    "last_name",
    "loan_id",
    "loan_category",
    "nr_of_months",
    "due_amount",
    "due_date",
    "payment_date",
]

LOAN_NR_OF_MONTHS_RANGE = {"min": 6, "max": 50}
LOAN_DUE_DAY_OF_MONTH_RANGE = {"min": 22, "max": 26}
LOAN_PAYMENT_DAY_OF_MONTH_RANGE = {"min": 1, "max": 28}

LOAN_MIN_COST_VALUE_IN_USD = 100
LOAN_TYPES_WITH_MAX_COST_VALUE_IN_USD = {"Personal": 200, "Auto": 300, "Student": 400}

OUTPUT_FILE_NAME = f"{FOLDER_NAME_FOR_FILES}/loans"


@dataclass
class Loan:
    customer_id: int
    first_name: str
    last_name: str
    loan_id: str
    loan_category: str
    start_dates_index: int
    nr_of_months: int
    due_amount: int
    due_day: int


def _generate_loan_dates_indexes(dates: list[tuple[Any, ...]]) -> tuple[int, int]:
    total_nr_of_months = len(dates)
    loan_length_in_months = random.randint(
        LOAN_NR_OF_MONTHS_RANGE["min"], LOAN_NR_OF_MONTHS_RANGE["max"]
    )
    last_accurate_month_nr = total_nr_of_months - loan_length_in_months
    start_month_index = random.randint(0, last_accurate_month_nr)
    return start_month_index, loan_length_in_months


def extend_loans_info(
    base_customers_info: list[tuple[Any, ...]], dates: list[tuple[Any, ...]]
) -> list[Loan]:
    base_infos = []

    for base_customer in base_customers_info:
        customer_id, first_name, last_name = base_customer
        loan_id = uuid.uuid4().hex
        loan_category = random.choice(
            list(LOAN_TYPES_WITH_MAX_COST_VALUE_IN_USD.keys())
        )
        max_due_amount_value = LOAN_TYPES_WITH_MAX_COST_VALUE_IN_USD[loan_category]
        start_dates_index, loan_length_in_months = _generate_loan_dates_indexes(dates)
        due_amount = random.randint(LOAN_MIN_COST_VALUE_IN_USD, max_due_amount_value)
        due_day = random.randint(
            LOAN_DUE_DAY_OF_MONTH_RANGE["min"], LOAN_DUE_DAY_OF_MONTH_RANGE["max"]
        )
        loan = Loan(
            customer_id,
            first_name,
            last_name,
            loan_id,
            loan_category,
            start_dates_index,
            loan_length_in_months,
            due_amount,
            due_day,
        )
        base_infos.append(loan)
    return base_infos


def data_generator(
    loan: Loan, dates: list[tuple[Any, ...]]
) -> Iterator[dict[str, Any]]:
    loan_months = dates[
        loan.start_dates_index : loan.nr_of_months + loan.start_dates_index
    ]
    record = asdict(loan)
    del record["start_dates_index"]
    del record["due_day"]

    for raw_month in loan_months:
        record_to_write = record
        date_without_day = raw_month[0][2:]
        full_due_date = f"{loan.due_day}{date_without_day}"
        record_to_write["due_date"] = full_due_date
        payment_day = random.randint(
            LOAN_PAYMENT_DAY_OF_MONTH_RANGE["min"],
            LOAN_PAYMENT_DAY_OF_MONTH_RANGE["max"],
        )
        full_payment_date = f"{payment_day}{date_without_day}"
        record_to_write["payment_date"] = full_payment_date
        yield record_to_write


if __name__ == "__main__":
    reader = Reader()
    base_customers_info = reader.read_data_from_file(CUSTOMERS_FILENAME)
    dates = reader.read_data_from_file(DATES_FILENAME)
    loans = extend_loans_info(base_customers_info, dates)

    writer = Writer(OUTPUT_FILE_NAME, data_generator, loans, dates)
    writer.generate_data_as_temp_files()
    filenames = glob.glob("*.out")
    writer.merge_temp_files_as_csv(HEADERS, filenames)
    writer.delete_temp_files(filenames)
