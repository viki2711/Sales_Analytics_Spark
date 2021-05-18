# function that cleans the text file
# Stripping each line of curly brackets, new line and comma at the end of the line
# Splitting by comma to the smaller elements
# Splitting each element by semi-colon and taking just the value element
# Stripping each element from unnecessary spaces
# Appending each line to the new clean data list

def clean_orders(file):
    with open(file, "r") as file:
        orders = []
        for line in file:
            new_line = line.strip(",{}\n").split(",")
            order = [item.split(':')[1] for item in new_line]
            order = [item.strip() for item in order]
            orders.append(order)
    return orders


# file_ = "data/orders.txt"
# clean_orders_file = clean_orders(file_)
# for i in range(10):
#     print(clean_orders_file[i])
