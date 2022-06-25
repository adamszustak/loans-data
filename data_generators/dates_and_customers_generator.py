import argparse
import datetime
from typing import NoReturn, Union

from dateutil.rrule import MONTHLY, rrule
from faker import Faker

FOLDER_NAME_FOR_FILES = "output"
DATES_FILENAME = f"{FOLDER_NAME_FOR_FILES}/dates.txt"
CUSTOMERS_FILENAME = f"{FOLDER_NAME_FOR_FILES}/customers.txt"
DATES_MIN_VALUE_IN_YEARS = CUSTOMERS_MIN_VALUE = 1


def generate_base_customers_data(nr_of_customers: int) -> list[str]:
    customers = []
    for _ in range(nr_of_customers):
        customer_id = fake.isbn10(separator="")
        first_name = fake.first_name()
        last_name = fake.last_name()
        customers.append(", ".join([customer_id, first_name, last_name]) + "\n")
    return customers


def generate_dates(nr_of_years: int) -> list[str]:
    year_to = datetime.date.today().year - 1
    date_to = datetime.date(year_to, 12, 1)
    year_from = year_to - (nr_of_years - 1)
    date_from = datetime.date(year_from, 1, 1)

    dates = [
        f"{dt.strftime('%d-%m-%Y')}\n"
        for dt in rrule(MONTHLY, dtstart=date_from, until=date_to)
    ]
    return dates


def write_to_file(data: list[str], filename: str) -> None:
    with open(filename, "w") as file:
        file.writelines(data)


def int_with_min_value(min_value: int) -> Union[NoReturn, int]:
    def int_checker(arg):
        try:
            f = int(arg)
        except ValueError:
            raise argparse.ArgumentTypeError("Must be a integer")
        if f < min_value:
            raise argparse.ArgumentTypeError(
                f"Argument must be greater than {min_value}"
            )
        return f

    return int_checker


def parse_arguments() -> tuple[int, int]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "customer", type=int_with_min_value(CUSTOMERS_MIN_VALUE), help="Nr of customers"
    )
    parser.add_argument(
        "years", type=int_with_min_value(DATES_MIN_VALUE_IN_YEARS), help="Nr of years"
    )
    args = parser.parse_args()
    return args.customer, args.years


if __name__ == "__main__":
    fake = Faker()
    nr_of_customers, nr_of_years = parse_arguments()

    customers = generate_base_customers_data(nr_of_customers)
    write_to_file(customers, CUSTOMERS_FILENAME)
    dates = generate_dates(nr_of_years)
    write_to_file(dates, DATES_FILENAME)
