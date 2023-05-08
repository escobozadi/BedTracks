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
                                    (90, 113), (113, 117), (117, 133), (133, 138), (138, 148),
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

    dataFrame = dataFrame.fillna("")
    dataFrame["DIST-PERIOD-START"] = pd.to_datetime(dataFrame["DIST-PERIOD-START"], format="%Y%m%d")
    dataFrame["DIST-PERIOD-END"] = pd.to_datetime(dataFrame["DIST-PERIOD-END"], format="%Y%m%d")
    dataFrame["EARNINGS-AMOUNT"] = dataFrame["EARNINGS-AMOUNT"].astype(float) / 10000
    dataFrame["DURATION"] = "00000000" + dataFrame["DURATION"].astype(str).str[:-2]
    dataFrame["DURATION"] = dataFrame["DURATION"].str[-6:-4] + ":" + \
                            dataFrame["DURATION"].str[-4:-2] + ":" + dataFrame["DURATION"].str[-2:]
    dataFrame = dataFrame.replace({"DURATION": {"00:00:00": np.nan}})
    dataFrame["SOURCE"] = dataFrame["SOURCE"] + dataFrame["STATION"]
    dataFrame["SHARE-PERCENT"] = dataFrame["SHARE-PERCENT"].astype(float) / 1000

    pd.set_option("display.max_columns", len(dataFrame))
    print(dataFrame)
    print(dataFrame.iloc[[25580]])
    pd.reset_option("display.max_columns")
    dataFrame.to_csv("./B01_SOCAN_2012toJun2022_Dat.csv", encoding="utf-8", index=False)


def cleanDataFrames():
    df1 = pd.DataFrame(pd.read_csv("./B01_SOCAN_2012toJun2022_Dat.csv", index_col=False))
    df2 = pd.DataFrame(pd.read_csv("./B01_SOCAN_2012toJun2022.csv", index_col=False))

    # DataFrame #1
    # df1 = df1.fillna("")
    # df1["DIST-PERIOD-START"] = pd.to_datetime(df1["DIST-PERIOD-START"], format="%Y%m%d")
    # df1["DIST-PERIOD-END"] = pd.to_datetime(df1["DIST-PERIOD-END"], format="%Y%m%d")
    # df1["EARNINGS-AMOUNT"] = df1["EARNINGS-AMOUNT"].astype(float) / 10000
    # df1["DURATION"] = "00000000" + df1["DURATION"].astype(str).str[:-2]
    # df1["DURATION"] = df1["DURATION"].str[-6:-4] + ":" + df1["DURATION"].str[-4:-2] + ":" + df1["DURATION"].str[-2:]
    df1 = df1.replace({"SOURCE": {"ISPOT": "ISPOTIFY"}})
    # df1["SOURCE"] = df1["SOURCE"] + df1["STATION"]

    # DataFrame #2
    df2 = df2.fillna("")
    df2 = df2.rename(columns={" Work Number": "Work Number"})
    df2["Work Number"] = pd.to_numeric(df2["Work Number"])
    df2["Distribution Date"] = pd.to_datetime(df2["Distribution Date"])
    df2["Dist. Period Start"] = pd.to_datetime(df2["Dist. Period Start"])
    df2["Dist. Period End"] = pd.to_datetime(df2["Dist. Period End"])

    return df1, df2


if __name__ == '__main__':
    # datFormat()

    # sql = SQLConnect("localhost", "root", "Dakota.2486Nala")
    # sql.create_connection()
    # create_database_query = "CREATE DATABASE Records"
    # sql.create_database(create_database_query)

    df1, df2 = cleanDataFrames()
    # pd.set_option("display.max_columns", len(df1))

    # df1["SOURCE"] = df1["SOURCE"].replace({"ISPOT": "ISPOTIFY"})
    # df1.replace({"SOURCE": {"ISPOT": "ISPOTIFY"}})

    # df2["Duration"] = pd.to_datetime(df2["Duration"], format="%H:%M:%S")

    # print(df1.dtypes)
    # print(df2.dtypes)

    # print(df1[["SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE", "DURATION", "EARNINGS-AMOUNT"]])
    # print(df2[["Work Number", "Use", "Station", "Source", "Duration", "Amount"]])
    # print(df1.groupby(["SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE", "DURATION",
    #                    "EARNINGS-AMOUNT", "ISWC", "SHARE-PERCENT"]).size())
    # print(df2.groupby(["Work Number", "Use", "Station", "Source", "Duration", "Amount", "ISWC"]).size())

    # df1.set_index(["SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE", "DURATION", "EARNINGS-AMOUNT"], verify_integrity=True)
    print("DATAFRAME #3 ------------------------------------------------------------------------------------")
    # df3 = pd.merge(df1, df2, left_on=["SONG-TITLE", "SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE"],
    #                right_on=["Work Title", "Work Number", "Use", "Station", "Source"],
    #                how='left')

    # df3 = df2.join(df1.set_index(["SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE", "DURATION", "EARNINGS-AMOUNT", "ISWC"]),
    #                on=["Work Number", "Use", "Station", "Source", "Duration", "Amount", "ISWC"],
    #                how='inner', lsuffix="BedTrack", rsuffix="SOCAN")
    # print(df3.dtypes)
    # print(df3)

    # pd.reset_option("display.max_columns")

