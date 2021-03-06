import glob
import random
import uuid
from dataclasses import (
    asdict,
    dataclass
)
from typing import (
    Any,
    Iterator
)

from utils import (
    CUSTOMERS_FILENAME,
    DATES_FILENAME,
    FOLDER_NAME_FOR_FILES,
    Reader,
    Writer
)

HEADERS = [
    "customer_id",
    "first_name",
    "last_name",
    "loan_id",
    "loan_category",
    "nr_of_months",
    "due_amount_in_usd",
    "due_date",
    "payment_date",
    "payment_amount_in_usd",
]

LOAN_NR_OF_MONTHS_RANGE = {"min": 6, "max": 50}
LOAN_DUE_DAY_OF_MONTH_RANGE = {"min": 22, "max": 26}
LOAN_PAYMENT_DAY_OF_MONTH_RANGE = {"min": 1, "max": 28}

LOAN_MIN_COST_VALUE_IN_USD = 100
LOAN_TYPES_WITH_MAX_COST_VALUE_IN_USD = {"Personal": 200, "Auto": 300, "Student": 400}
LACK_OF_PAYMENT_PROBABILITY = 0.01
PAYMENT_DEVIATION_IN_PERCENTS = 0.1

OUTPUT_FILE_NAME = f"{FOLDER_NAME_FOR_FILES}/loans"
PATH_TO_AVRO_SCHEMA = "avro/loan.avsc"


@dataclass
class Loan:
    customer_id: str
    first_name: str
    last_name: str
    loan_id: str
    loan_category: str
    start_dates_index: int
    nr_of_months: int
    due_amount_in_usd: int
    due_day: int
    min_payment_amount_in_usd: int
    max_payment_amount_in_usd: int


def _generate_loan_dates_indexes(dates: list[tuple[Any, ...]]) -> tuple[int, int]:
    total_nr_of_months = len(dates)
    loan_length_in_months = random.randint(
        LOAN_NR_OF_MONTHS_RANGE["min"],
        min(LOAN_NR_OF_MONTHS_RANGE["max"], total_nr_of_months),
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
        due_amount_in_usd = random.randint(
            LOAN_MIN_COST_VALUE_IN_USD, max_due_amount_value
        )
        due_day = random.randint(
            LOAN_DUE_DAY_OF_MONTH_RANGE["min"], LOAN_DUE_DAY_OF_MONTH_RANGE["max"]
        )
        min_payment_amount_in_usd = int(due_amount_in_usd - due_amount_in_usd * PAYMENT_DEVIATION_IN_PERCENTS)
        max_payment_amount_in_usd = int(due_amount_in_usd + due_amount_in_usd * PAYMENT_DEVIATION_IN_PERCENTS)
        loan = Loan(
            customer_id,
            first_name,
            last_name,
            loan_id,
            loan_category,
            start_dates_index,
            loan_length_in_months,
            due_amount_in_usd,
            due_day,
            min_payment_amount_in_usd,
            max_payment_amount_in_usd
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
    del record["min_payment_amount_in_usd"]
    del record["max_payment_amount_in_usd"]

    for raw_month in loan_months:
        record_to_write = record.copy()
        date_without_day = raw_month[0][2:]
        full_due_date = f"{loan.due_day}{date_without_day}"
        record_to_write["due_date"] = full_due_date
        payment_day = random.randint(
            LOAN_PAYMENT_DAY_OF_MONTH_RANGE["min"],
            LOAN_PAYMENT_DAY_OF_MONTH_RANGE["max"],
        )
        full_payment_date = f"{payment_day}{date_without_day}"
        record_to_write["payment_date"] = full_payment_date

        record_to_write["payment_amount"] = 0
        if random.random() > LACK_OF_PAYMENT_PROBABILITY:
            record_to_write["payment_amount"] = random.randint(loan.min_payment_amount_in_usd, loan.max_payment_amount_in_usd)
        yield record_to_write


if __name__ == "__main__":
    output_extension = Writer.parse_arguments()

    reader = Reader()
    base_customers_info = reader.read_data_from_file(CUSTOMERS_FILENAME)
    dates = reader.read_data_from_file(DATES_FILENAME)
    loans = extend_loans_info(base_customers_info, dates)

    writer = Writer(OUTPUT_FILE_NAME, data_generator, loans, dates)
    if output_extension == "csv":
        writer.generate_data_as_temp_files()
        filenames = glob.glob("*.out")
        writer.merge_temp_files_as_csv(HEADERS, filenames)
        writer.delete_temp_files(filenames)
    elif output_extension == "avro":
        writer.generate_data_as_avro(PATH_TO_AVRO_SCHEMA)
