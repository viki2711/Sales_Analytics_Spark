import clean_orders
from helpers import parse_int, parse_string, parse_float

# Tuple of parse functions according to the spark schema of each order
parse_func_order = (parse_string, parse_string, parse_int, parse_int, parse_int, parse_string, parse_float)


# Parsing each element in one order to the correct data type, according to the spark schema
def parse_order(fields):
    new_data = (func(field) for func, field in zip(parse_func_order, fields))
    return new_data


# Loop through each order, parse it and append it to the new list of parsed data.
def parse_orders(func, arr):
    parsed = []
    for line in arr:
        parsed_data = func(line)
        parsed.append(list(parsed_data))
    return parsed


# file_ = "data/orders.txt"
# cleaned_orders = clean_orders.clean_orders(file_)
# parsed_ = parse_orders(parse_order, cleaned_orders)
# print(parsed_)
