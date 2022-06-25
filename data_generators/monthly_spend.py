import glob
import random
from dataclasses import asdict, dataclass
from typing import Any, Iterator

from utils import (CUSTOMERS_FILENAME, DATES_FILENAME, FOLDER_NAME_FOR_FILES,
                   Reader, Writer)

HEADERS = [
    "customer_id",
    "first_name",
    "last_name",
    "spend_rate",
    "available_money_rate",
    "spend_money_in_usd",
    "available_money_in_usd",
    "month",
]

TOTAL_SPEND_RATES_IN_USD = TOTAL_AVAILABLE_MONEY_RATES_IN_USD = {
    "High": "2001-10000",
    "Medium": "501-2000",
    "Low": "10-500",
}
CHANGE_SPEND_RATE_PROBABILITY = 0.3
CHANGE_AVAILABLE_MONEY_RATE_PROBABILITY = 0.1

OUTPUT_FILE_NAME = f"{FOLDER_NAME_FOR_FILES}/monthly_spend"


@dataclass
class Customer:
    customer_id: str
    first_name: str
    last_name: str
    spend_rate: str
    available_money_rate: str
    spend_money_in_usd: int
    available_money_in_usd: int


def extend_base_customers_info(
    base_customers_info: list[tuple[Any, ...]]
) -> list[Customer]:
    base_infos = []
    for base_customer in base_customers_info:
        customer_id, first_name, last_name = base_customer
        spend_rate = random.choice(tuple(TOTAL_SPEND_RATES_IN_USD.keys()))
        available_money_rate = random.choice(
            tuple(TOTAL_AVAILABLE_MONEY_RATES_IN_USD.keys())
        )
        spend_money_in_usd = _get_number_based_on_rate(
            TOTAL_SPEND_RATES_IN_USD, spend_rate
        )
        available_money_in_usd = _get_number_based_on_rate(
            TOTAL_AVAILABLE_MONEY_RATES_IN_USD, available_money_rate
        )
        customer = Customer(
            customer_id,
            first_name,
            last_name,
            spend_rate,
            available_money_rate,
            spend_money_in_usd,
            available_money_in_usd,
        )
        base_infos.append(customer)
    return base_infos


def _get_number_based_on_rate(rates_with_ranges: dict[str, str], rate: str) -> int:
    rate_range = rates_with_ranges[rate]
    nr_from, nr_to = rate_range.split("-")
    return random.randint(int(nr_from), int(nr_to))


def data_generator(
    customer: Customer, dates: list[tuple[Any, ...]]
) -> Iterator[dict[str, Any]]:
    for date in dates:
        unpacked_date = date[0]
        if random.random() < CHANGE_SPEND_RATE_PROBABILITY:
            customer.spend_rate = random.choice(list(TOTAL_SPEND_RATES_IN_USD.keys()))
        if random.random() < CHANGE_AVAILABLE_MONEY_RATE_PROBABILITY:
            customer.available_money_rate = random.choice(
                list(TOTAL_AVAILABLE_MONEY_RATES_IN_USD.keys())
            )

        customer.spend_money_in_usd = _get_number_based_on_rate(
            TOTAL_SPEND_RATES_IN_USD, customer.spend_rate
        )
        customer.available_money_in_usd = _get_number_based_on_rate(
            TOTAL_AVAILABLE_MONEY_RATES_IN_USD, customer.available_money_rate
        )
        raw_base_info = asdict(customer)
        raw_base_info["month"] = unpacked_date
        yield raw_base_info


if __name__ == "__main__":
    reader = Reader()
    base_customers_info = reader.read_data_from_file(CUSTOMERS_FILENAME)
    dates = reader.read_data_from_file(DATES_FILENAME)
    customers_info = extend_base_customers_info(base_customers_info)

    writer = Writer(OUTPUT_FILE_NAME, data_generator, customers_info, dates)
    writer.generate_data_as_temp_files()
    filenames = glob.glob("*.out")
    writer.merge_temp_files_as_csv(HEADERS, filenames)
    writer.delete_temp_files(filenames)
