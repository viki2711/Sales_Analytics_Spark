# Parsing functions
# Function for parsing value to integer
def parse_int(value):
    return int(value)


# Function for parsing value to float
def parse_float(value):
    return float(value)


# Function for parsing value to a clean string
def parse_string(value):
    return value.replace('"', '')