import json


# Function for parsing value to integer
def parse_int(value):
    return int(value)


# Function for parsing value to float
def parse_float(value):
    return float(value)


# Function for parsing value to a clean string
def parse_string(value):
    return value.replace('"', '')


# open a json file
def open_json(path):
    with open(path, "r") as file:
        data = json.load(file)
    return data