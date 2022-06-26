import argparse
import copy
import json
import multiprocessing
import os
from typing import (
    Any,
    Callable
)

import fastavro

BATCH_WRITE_SIZE = 5000

FOLDER_NAME_FOR_FILES = "output"
DATES_FILENAME = f"{FOLDER_NAME_FOR_FILES}/dates.txt"
CUSTOMERS_FILENAME = f"{FOLDER_NAME_FOR_FILES}/customers.txt"


class Reader:
    def read_data_from_file(self, filename: str) -> list[tuple[Any, ...]]:
        elements = []
        with open(filename, "r") as file:
            for row in file:
                elements.append(tuple(row.rstrip().split(",")))
        return elements

    def read_data_from_avro(self, filename: str) -> list[dict[str, Any]]:
        with open(filename, "rb") as f:
            reader = fastavro.reader(f)
            content = [el for el in reader]
            metadata = copy.deepcopy(reader.metadata)
            print(f"Schema for AVRO file:\n{json.loads(metadata['avro.schema'])}")
        return content


class Writer:
    def __init__(
        self,
        out_filename: str,
        data_generator: Callable,
        elements_to_write: list[Any],
        dates: list[tuple[Any, ...]],
    ) -> None:
        self.out_filename = out_filename
        self._data_generator = data_generator
        self._elements_to_write = elements_to_write
        self._dates = dates

    def _get_data_as_temp_file(self, element: Any) -> None:
        records_to_write = []

        with open(f"{str(os.getpid())}.out", "a") as file:
            for raw_record_to_write in self._data_generator(element, self._dates):
                raw_record_to_write = list(raw_record_to_write.values())
                record_to_write = [str(field) for field in raw_record_to_write]
                records_to_write.append(f"{','.join(record_to_write)}\n")
                if len(records_to_write) % BATCH_WRITE_SIZE == 0:
                    file.writelines(records_to_write)
                    records_to_write = []
            file.writelines(records_to_write)

    def generate_data_as_temp_files(self) -> None:
        pool = multiprocessing.Pool()
        processes = [
            pool.apply_async(
                self._get_data_as_temp_file,
                args=(element,),
            )
            for element in self._elements_to_write
        ]
        [p.get() for p in processes]

    def merge_temp_files_as_csv(
        self, headers: list[str], temp_filenames: list[str]
    ) -> None:
        extension = "csv"

        with open(f"{self.out_filename}.{extension}", "w") as outfile:
            outfile.write(", ".join(headers) + "\n")
            for fname in temp_filenames:
                with open(fname, "r") as readfile:
                    infile = readfile.read()
                    outfile.write(infile)

    def generate_data_as_avro(
        self,
        path_to_schema: str,
    ) -> None:
        extension = "avro"
        parsed_schema = fastavro.schema.load_schema(path_to_schema)
        records_to_write = []

        with open(f"{self.out_filename}.{extension}", "a+b") as outfile:
            for element in self._elements_to_write:
                for record_to_write in self._data_generator(element, self._dates):
                    if len(records_to_write) % BATCH_WRITE_SIZE == 0:
                        fastavro.writer(outfile, parsed_schema, records_to_write)
                        records_to_write = []
                    records_to_write.append(record_to_write)
            fastavro.writer(outfile, parsed_schema, records_to_write)

    def delete_temp_files(self, temp_filenames: list[str]) -> None:
        for filePath in temp_filenames:
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)

    @staticmethod
    def parse_arguments() -> tuple[int, int]:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "extension",
            type=str,
            help="Type of output file. Possible values: AVRO, CSV",
            choices=("csv", "avro"),
        )
        args = parser.parse_args()
        return args.extension
