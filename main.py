"""
Code to analyze SOCAN Royalty statements
"""
import csv
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
from tabulate import tabulate


class SQLConnect(object):

    def __init__(self, host_name, user_name, passw):
        self.host_name = host_name
        self.user_name = user_name
        self.password = passw
        self.connection = None

    def create_connection(self):
        self.connection = None
        try:
            self.connection = mysql.connector.connect(
                host=self.host_name,
                user=self.user_name,
                passwd=self.password
            )
            print("Connection successful!")
        except Error as err:
            print(f"Error: '{err}'")

    def create_database(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            print("Database creation successful!")
        except Error as err:
            print(f"Error: '{err}'")

    def connect_db(self, db_name):
        self.connection = None
        try:
            self.connection = mysql.connector.connect(
                host=self.host_name,
                user=self.user_name,
                passwd=self.password,
                database=db_name
            )
            print("Databse Connection Successful!")
        except Error as err:
            print(f"Error: '{err}'")

    def execute_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            print("Query successful!")
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
    # pd.set_option("display.max_columns", len(dataFrame))
    # print(dataFrame)
    # pd.reset_option("display.max_columns")
    dataFrame.to_csv("./dat_csv.csv", encoding="utf-8", index=False)


def cleanDataFrames():
    df1 = pd.DataFrame(pd.read_csv("./dat_csv.csv", index_col=False))
    df2 = pd.DataFrame(pd.read_csv("./B01_SOCAN_2012toJun2022.csv", index_col=False))

    # DataFrame #1
    df1["DIST-PERIOD-START"] = pd.to_datetime(df1["DIST-PERIOD-START"], format="%Y%m%d")
    df1["DIST-PERIOD-END"] = pd.to_datetime(df1["DIST-PERIOD-END"], format="%Y%m%d")

    # DataFrame #2
    df2 = df2.rename(columns={" Work Number": "Work Number"})
    df2["Work Number"] = pd.to_numeric(df2["Work Number"])

    df2["Distribution Date"] = pd.to_datetime(df2["Distribution Date"])
    df2["Dist. Period Start"] = pd.to_datetime(df2["Dist. Period Start"])
    df2["Dist. Period End"] = pd.to_datetime(df2["Dist. Period End"])

    return


if __name__ == '__main__':
    # datFormat()
    # sql = SQLConnect("localhost", "root", "Dakota.2486Nala")
    # sql.create_connection()
    # create_database_query = "CREATE DATABASE Records"
    # sql.create_database(create_database_query)

    df1 = pd.DataFrame(pd.read_csv("./dat_csv.csv", index_col=False))
    df2 = pd.DataFrame(pd.read_csv("./B01_SOCAN_2012toJun2022.csv", index_col=False))
    pd.set_option("display.max_columns", len(df1))

    print(df1.dtypes)
    print(df1)

    print(df2.dtypes)
    print(df2)

    df2 = df2.rename(columns={" Work Number": "Work Number"})

    df1["DIST-PERIOD-START"] = pd.to_datetime(df1["DIST-PERIOD-START"], format="%Y%m%d")
    df1["DIST-PERIOD-END"] = pd.to_datetime(df1["DIST-PERIOD-END"], format="%Y%m%d")
    print(df1["DIST-PERIOD-START"])
    df2["Distribution Date"] = pd.to_datetime(df2["Distribution Date"])
    df2["Dist. Period Start"] = pd.to_datetime(df2["Dist. Period Start"])
    df2["Dist. Period End"] = pd.to_datetime(df2["Dist. Period End"])

    df2["Work Number"] = pd.to_numeric(df2["Work Number"])

    df3 = pd.merge(df1, df2, left_on=["SONG-CODE"],
                   right_on=["Work Number"], how='left')
    print(df3)

    pd.reset_option("display.max_columns")

