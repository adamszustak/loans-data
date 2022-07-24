from datetime import datetime
from typing import (
    Dict,
    Iterable,
    Tuple,
    Union
)

import apache_beam as beam

HEADERS = ("customer_id", "cause-points", "total points")


def get_sum_due_and_payment_amount(
    values: Tuple[int, int],
) -> Tuple[int, int]:
    paid_amount, loan_amount = 0, 0
    for row in values:
        paid_amount += row[0]
        loan_amount += row[1]
    return paid_amount, loan_amount


def count_points_for_not_paid_loan(
    row: Tuple[str, Tuple[int, int]],
) -> Tuple[str, int]:
    points = 0
    balance_amount = row[1][1] - row[1][0]
    if balance_amount > 0:
        points += 3
    return row[0], points


def count_points_for_lack_of_payments(
    row: Tuple[str, Iterable[int]]
) -> Tuple[str, int]:
    points = 0
    key, values = row[0], row[1]
    if len(values) > 2:
        points = 1
    return key, points


def count_points_for_late_payment(
    row: Iterable[str],
) -> Iterable[Union[str, int]]:
    raw_due_date, raw_payment_date = row[7], row[8]
    due_date_to_compare = datetime.strptime(raw_due_date, "%d-%m-%Y")
    payment_date_to_compare = datetime.strptime(raw_payment_date, "%d-%m-%Y")
    if payment_date_to_compare <= due_date_to_compare:
        row.append(0)
    else:
        row.append(1)
    return row


def filter_where_available_money_is_higher_than_due_amount(
    row: Tuple[str, Dict[str, Iterable[str]]]
) -> Tuple[str, Dict[str, Iterable[str]]]:
    loan_amount = row[1]["loans"]
    available_amount = row[1]["available money"][0]
    if loan_amount and (int(loan_amount[0]) < int(available_amount)):
        return row


def prepare_row_key_with_month(
    row: Iterable[str], date_index: int, amount_index: int
) -> Tuple[str, str]:
    raw_date, amount = row[date_index], row[amount_index]
    date_to_compare = datetime.strptime(raw_date, "%d-%m-%Y")
    date_without_day = datetime(
        date_to_compare.year, date_to_compare.month, 1
    ).strftime("%m%Y")
    return f"{row[0]}/{date_without_day}", amount


def count_points_for_lack_of_full_amount_payment_on_account_available_money(
    row: Tuple[str, int]
) -> Tuple[str, int]:
    key, value = row
    points = value // 3
    return key, points


def format_output(
    row: Tuple[Union[str, int], ...], cause: str
) -> Tuple[Union[str, int], ...]:
    key, value = row
    return key, value, cause


def summarize_output(
    row: Tuple[Union[int, str], Iterable[Tuple[Union[int, str], ...]]]
) -> Tuple[str, int]:
    key, values = row
    output = f"{key}, "
    total_points = 0
    for value in values:
        points, cause = value
        if points:
            output += f"{cause}-{points} "
            total_points += points
    return f"{output}, {total_points}", total_points


class SplitEveryRow(beam.PTransform):
    def expand(
        self, input_pipe: beam.pvalue.PCollection[str]
    ) -> beam.pvalue.PCollection[Iterable[str]]:
        output = (
            input_pipe
            | "Split each row" >> beam.Map(lambda row: row.split(","))
            | "Remove blank lines" >> beam.Map(lambda row: [el.strip() for el in row])
        )
        return output


def run_pipeline():
    with beam.Pipeline() as pipe:
        loans_input_splitted = (
            pipe
            | "Read loans data"
            >> beam.io.ReadFromText(
                "../data_generators/output/loans.csv", skip_header_lines=1
            )
            | "Split loans data" >> SplitEveryRow()
        )
        monthly_spend_splitted = (
            pipe
            | "Read monthly spend data"
            >> beam.io.ReadFromText(
                "../data_generators/output/monthly_spend.csv", skip_header_lines=1
            )
            | "Split monthly spend data" >> SplitEveryRow()
        )
        loan_debtor = (
            loans_input_splitted
            | "id: due_amount_in_usd, payment_amount_in_usd"
            >> beam.Map(lambda row: (f"{row[0]}", (int(row[6]), int(row[9]))))
            | "Get sum of due and payment amounts"
            >> beam.CombinePerKey(get_sum_due_and_payment_amount)
            | "Count points for not fully paid loans"
            >> beam.Map(count_points_for_not_paid_loan)
            | "Prepare output for loan debtor"
            >> beam.Map(format_output, cause="debtor")
        )
        loan_skippers = (
            loans_input_splitted
            | "Filter where payment_amount_in_usd is 0"
            >> beam.Filter(lambda row: row[9] == "0")
            | "id: 1" >> beam.Map(lambda row: (f"{row[0]}", 1))
            | "Group amount of skipped payments" >> beam.GroupByKey()
            | "Count points for lack of payments"
            >> beam.Map(count_points_for_lack_of_payments)
            | "Prepare output for loan skippers"
            >> beam.Map(format_output, cause="skipper")
        )
        loan_late_payments = (
            loans_input_splitted
            | "Count points for too late payments"
            >> beam.Map(count_points_for_late_payment)
            | "id: point" >> beam.Map(lambda row: (row[0], row[10]))
            | "Sum late payments" >> beam.CombinePerKey(sum)
            | "Prepare output for late payment"
            >> beam.Map(format_output, cause="late payment")
        )
        not_full_paid_loans = (
            loans_input_splitted
            | "Filter not full paid loans"
            >> beam.Filter(lambda row: int(row[9]) < int(row[6]))
            | "id/date: payment"
            >> beam.Map(prepare_row_key_with_month, date_index=7, amount_index=9)
        )
        available_money_in_each_month = (
            monthly_spend_splitted
            | "id/date: available money"
            >> beam.Map(prepare_row_key_with_month, date_index=7, amount_index=6)
        )
        loan_lack_of_full_amount_payment_but_on_account_available_money = (
            {
                "loans": not_full_paid_loans,
                "available money": available_money_in_each_month,
            }
            | "Group not full paid loans despite available money per month"
            >> beam.CoGroupByKey()
            | "Filter entries where available money is higher than due amount"
            >> beam.Filter(filter_where_available_money_is_higher_than_due_amount)
            | "Remove month from key; id1: 1"
            >> beam.Map(lambda row: (row[0].split("/")[0], 1))
            | "Sum not full paid loans" >> beam.CombinePerKey(sum)
            | "Count points for lack of full payment where has enough money"
            >> beam.Map(
                count_points_for_lack_of_full_amount_payment_on_account_available_money
            )
            | "Prepare output for not full paid loan"
            >> beam.Map(format_output, cause="not full paid")
        )
        final_output = (
            (
                loan_debtor,
                loan_skippers,
                loan_late_payments,
                loan_lack_of_full_amount_payment_but_on_account_available_money,
            )
            | "Merge PCollections" >> beam.Flatten()
            | "id: points, cause" >> beam.Map(lambda row: (row[0], row[1:]))
            | "Group points and causes" >> beam.GroupByKey()
            | "Final output" >> beam.Map(summarize_output)
            | "Filter id where exists any point" >> beam.Filter(lambda row: row[1] > 0)
            | "Get final output" >> beam.Map(lambda row: row[0])
            | "Write output to a file"
            >> beam.io.WriteToText(
                file_path_prefix="outputs/final",
                file_name_suffix=".csv",
                header=", ".join(HEADERS),
            )
        )


if __name__ == "__main__":
    run_pipeline()
