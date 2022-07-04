# Loans Project

## Table of contents
- [General info](#general-info)
  * [Rules](#rules)
- [Technologies](#technologies)
- [Setup](#setup)
- [ToDo](#todo)


## General info

The project aims to verify customer loan repayments based on the generated monthly data.

The project contains two main sections:

**data_generators** - module for generating fake data. Contains:
- monthly_spend.py - module for generating card expenses per month based on textfiles (customer and date) from *dates_and_customers_generator.py* module
- loans.py - module for generating loans per month based on textfiles (customer and date) from *dates_and_customers_generator.py* module

**beam_pipeline.py** - module contains a Beam pipeline for clients verifications by assigning points to them.

## Rules

* loans module contains only full length loans (per month)
* if the customer has not paid the loan in full => plus 3 points
* if the customer has not paid more than two installments => plus 1 point
* if the customer was late with the loan repayment => plus 1 point for each month
* if the customer has not paid the entire loan installment, but has the money on the account => plus 1 point for every 3 months
	
## Technologies

Project is created with:
* Python
* Apache Beam
* Fastavro
	
## Setup

Example of usage:
```
python3 dates_and_customers_generator.py <nr_of_customers> <nr_of_years>
python3 monthly_spend.py  # generated rows in final file => nr_of_years * 12 * nr_of_customers
python3 loans.py  # generated rows between: 6 * nr_of_customers * nr_of_years => 50 * nr_of_customers * nr_of_years
```

Local Beam Pipeline run:
```
python3 beam_pipeline.py
```

## ToDo

* Add Avro support to Beam pipeline
* Add Dataflow and BigQuery support
