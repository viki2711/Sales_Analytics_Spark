
Description:

Sample data to work with:
* The item_data.json file provides the mapping information from the items purchased to item information.
* The orders.txt file contains the user, item, and price information.

Please answer the following questions based on the attached data:
    • Which user spent the most?
    • What was the most expensive item that was purchased by the user who spent the most?
    • How much did Marissa Washington Spend?
---------------------------------------------------
My Approach:
After the phone interview and in order to meet position requirements,
I decided to study the basics of Apache Spark.

For this project I wanted to use python, pyspark and pandas.
Please make sure that all of these are installed and configured.
---------------------------------------------------

To execute the program, run main.py and the answers will be printed out.

---------------------------------------------------
Data processing steps:

Orders:
Pyspark for creating a spark session and a DataFrame

In order to create a schema for a spark Data Frame from text file, I built 2 python functions:
    1. clean_orders - for cleaning the rows (in clean_orders.py)
    2. parse_orders - for parsing the elements to the correct data types (in parse_orders.py)
        * parse functions for each data type imported from helpers.py

After the Data Frame was created, I used transformations to filter the data, aggregations to sum the needed data,
and actions to execute the previous functions.

---------------------------------------------------

Items:
    1. Reading a json file (open function from helpers.py)
    2. Pandas for creating an items data frame

----------------------------------------------------

Steps I took in order to answer the given questions:

1. Which user spent the most?
    * Grouped the user fields
    * Summed the priced they paid
    * Created a new column for this sum
    * Ordered by the descending value
    * Executed the first() action on this data and fetched the needed value for the answer

The answer is:
The user that spent the most: Marissa Jefferson, account_id: 143, total: 105.09$.

2. What was the most expensive item that was purchased by the user who spent the most?
    * Selected the relevant fields
    * Where user id matched from previous function
    * Ordered by the item cost
    * Executed the first() action on this data and fetched the needed value for the answer
    * Fetched the item info from pandas data frame according to the item_id I got from previous execution.

The answer is:
The most expensive item that Marissa Jefferson purchased was: frozen short bow,
full item info - item_id: 2746, quality: 1, price = 99.99$.

3. How much did Marissa Washington Spend?
    * Selected the relevant fields
    * Where the name and last name were matched
    * Grouped by the user fields
    * Summed the paid priced
    * Created a new column for this sum
    * Collected all the users that were a match with the name for the answer

The answer is:
2 matching accounts were found:
    Marissa Washington, account_id: 145 , total: 26.97$.
    Marissa Washington, account_id: 125, total: 18.45$.







