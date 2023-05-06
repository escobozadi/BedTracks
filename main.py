"""
Code to modify SOCAN Publisher File into a table according to the layout
"""
import csv
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
from tabulate import tabulate


def create_connection(host_name, user_name, password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=password
        )
        print("Connection successful!")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database creation successful!")
    except Error as err:
        print(f"Error: '{err}'")


def datFormat():
    # Adding Format to .dat file and saving to .csv
    datFile = pd.read_fwf("B01_SOCAN_2012toJun2022.dat", header=None, index_col=False,
                          colspecs=[(0, 4), (4, 5), (5, 50), (50, 60), (60, 90),
                                    (90, 113), (113, 117), (117, 133), (133, 139), (139, 148),
                                    (148, 156), (156, 166), (166, 216), (216, 224), (224, 274),
                                    (274, 284), (284, 293), (293, 298), (298, 299), (299, 306),
                                    (307, 317), (316, 324), (324, 332), (332, 340), (340, 341),
                                    (341, 349), (349, 389), (389, 400), (400, 405), (405, 450),
                                    (450, 465), (465, 469)],
                          names=["DIST-YEAR", "DIST-QTR", "SUPPLIER-NAME", "SONG-CODE", "SONG-TITLE",
                                 "SONG-WRITER", "INCOME-TYPE", "EARNINGS-AMOUNT", "SHARE-PERCENT", "SOCIETY",
                                 "SUPPLIER-NO", "AV-WORK-NO", "AV-WORK-TITLE", "SERIES-NO", "SERIES-TITLE",
                                 "SOURCE", "PERF-COUNT", "PERF-QTR", "USAGE", "DURATION",
                                 "IPI NAME NO", "DIST-DATE", "DIST-PERIOD-START", "DIST-PERIOD-END", "SPECIAL-DIST",
                                 "PERF-DATE", "PERFORMERS", "ISWC", "STATION", "VENUE",
                                 "CITY", "PERF-TYPE"])
    dataFrame = pd.DataFrame(data=datFile)
    pd.set_option("display.max_columns", len(dataFrame))
    print(dataFrame)
    pd.reset_option("display.max_columns")

    dataFrame.to_csv("./dat_csv.csv", encoding="utf-8", index=False)


if __name__ == '__main__':
    # create_connection("localhost", "root", "Dakota.2486Nala")
    datFormat()

    # print(tabulate(dataFrame, headers="keys", tablefmt="psql"))
    # display = pd.options.display
    # display(dataFrame)
