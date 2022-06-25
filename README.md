***WIP**

**dates_and_customers_generator.py** - module for generating names and dates
**monthly_spend.py** - module for generating card expenses per month based on textfiles (customer and date) from *dates_and_customers_generator.py* module
**loans.py** - module for generating loans per month based on textfiles (customer and date) from *dates_and_customers_generator.py* module

example of usage:
```
python3 dates_and_customers_generator.py <nr_of_customers> <nr_of_years>
python3 monthly_spend.py  # generated rows in final file => nr_of_years * 12 * nr_of_customers
python3 loans.py  # generated rows between: 6 * nr_of_customers * nr_of_years => 50 * nr_of_customers * nr_of_years
```