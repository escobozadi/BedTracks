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
    dataFrame = dataFrame.replace({"SOURCE": {"ISPOT": "ISPOTIFY"}})
    dataFrame["DIST."] = dataFrame["DIST-QTR"].astype(str) + "Q" + dataFrame["DIST-YEAR"].astype(str).str[-2:]
    dataFrame["PERF-COUNT"] = pd.to_numeric(dataFrame["PERF-COUNT"])

    # pd.set_option("display.max_columns", len(dataFrame))
    # print(dataFrame)
    # pd.reset_option("display.max_columns")

    # Rearrange columns
    dataFrame = dataFrame[["DIST.", "DIST-DATE", "DIST-PERIOD-START", "DIST-PERIOD-END", "SPECIAL-DIST",
                           "SUPPLIER-NAME", "SUPPLIER-NO", "IPI NAME NO",
                           "SERIES-NO", "SERIES-TITLE", "AV-WORK-NO", "AV-WORK-TITLE",
                           "SONG-TITLE", "SONG-WRITER", "SONG-CODE",
                           "ISWC", "SHARE-PERCENT", "USAGE", "PERF-COUNT", "SOCIETY", "INCOME-TYPE",
                           "SOURCE", "DURATION", "PERF-DATE", "PERFORMERS", "VENUE", "CITY", "PERF-TYPE",
                           "EARNINGS-AMOUNT"]]
    dataFrame.to_csv("./B01_SOCAN_2012toJun2022_Dat.csv", encoding="utf-8", index=False)
    return


def cleanDataFrames():
    df1 = pd.DataFrame(pd.read_csv("./B01_SOCAN_2012toJun2022_Dat.csv", index_col=False))
    df2 = pd.DataFrame(pd.read_csv("./B01_SOCAN_2012toJun2022.csv", index_col=False))

    # DataFrame #1
    df1 = df1.fillna("")
    # df1["DIST-PERIOD-START"] = pd.to_datetime(df1["DIST-PERIOD-START"], format="%Y%m%d")
    # df1["DIST-PERIOD-END"] = pd.to_datetime(df1["DIST-PERIOD-END"], format="%Y%m%d")
    # df1["EARNINGS-AMOUNT"] = df1["EARNINGS-AMOUNT"].astype(float) / 10000
    # df1["DURATION"] = "00000000" + df1["DURATION"].astype(str).str[:-2]
    # df1["DURATION"] = df1["DURATION"].str[-6:-4] + ":" + df1["DURATION"].str[-4:-2] + ":" + df1["DURATION"].str[-2:]
    # df1 = df1.replace({"SOURCE": {"ISPOT": "ISPOTIFY"}})
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
    pd.set_option("display.max_columns", len(df1))

    # df1["DIST."] = df1["DIST-QTR"].astype(str) + "Q" + df1["DIST-YEAR"].astype(str).str[-2:]
    # print(df1)

    # print(df1.dtypes)
    # print(df2.dtypes)

    # print(df1[["DIST.", "SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE", "DURATION",
    #            "EARNINGS-AMOUNT", "ISWC", "SHARE-PERCENT"]])
    # print(df2[["Dist.", "Work Number", "Use", "Station", "Source", "Duration",
    #            "Amount", "ISWC", "Share %"]])
    # print(df1.groupby(["DIST.", "SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE", "DURATION",
    #                    "EARNINGS-AMOUNT", "SHARE-PERCENT"]).size())
    # print(df2.groupby(["Dist.", "Work Number", "Use", "Station", "Source", "Duration",
    #                    "Amount", "Share %"]).size())

    print(df1.shape)
    # print(df1.groupby(["DIST.", "SONG-CODE", "DURATION", "PERF-COUNT", "EARNINGS-AMOUNT", "ISWC",
    #                    "SUPPLIER-NAME", "SUPPLIER-NO", "SONG-TITLE", "SONG-WRITER",
    #                    "SERIES-TITLE", "AV-WORK-TITLE", "SOCIETY"]).size())
    # print(df2.groupby(["Dist.", "Work Number", "Duration", "Amount"]).size())

    print("Duplicates")
    print(df1.duplicated().sum())
    print(df2.duplicated().sum())
    print("\n")
    print("Earnings")
    print(df1["EARNINGS-AMOUNT"].sum())
    print(df2["Amount"].sum())

    # df1.set_index(["SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE",
    # "DURATION", "EARNINGS-AMOUNT"], verify_integrity=True)

    # print(df1.groupby(df1.columns.tolist(), as_index=False).size())
    # print(df1.groupby(df1.columns.tolist(), as_index=False).size()["size"])
    # print(df1)

    # df1["DUPLICATE-COUNT"] = df1.groupby(df1.columns.tolist(), as_index=False).size()["size"]
    # print(df1.iloc[[30003]])

    # print(df1[df1.groupby(df1.columns.tolist(), as_index=False).size()["size"]])
    # print(df1.groupby(df1.columns.tolist(), as_index=False).size()["size"] != 1.0)
    # print(df1.loc[df1.groupby(df1.columns.tolist(), as_index=False).size()["size"] != 1.0, :])
    df1_dup = df1[df1.duplicated(keep=False)]
    df2_dup = df2[df2.duplicated(keep=False)]

    df1 = df1.drop_duplicates()
    df2 = df2.drop_duplicates()
    # print("Duplicates")
    # print(df1.duplicated().sum())
    # print(df2.duplicated().sum())
    #
    # df1_dup.to_csv("./Dat_Duplicates.csv", encoding="utf-8", index=False)
    # df2_dup.to_csv("./CSV_Duplicates.csv", encoding="utf-8", index=False)
    # print(df1_dup)
    # print(df2_dup)
    print("DATAFRAME #3 ------------------------------------------------------------------------------------")
    # df3 = pd.merge(df1, df2, left_on=["SONG-TITLE", "SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE"],
    #                right_on=["Work Title", "Work Number", "Use", "Station", "Source"],
    #                how='left')

    # Join DAT into CSV
    df3 = df2.join(df1.set_index(["DIST.", "SUPPLIER-NAME", "SUPPLIER-NO", "SERIES-TITLE", "AV-WORK-TITLE",
                                  "SONG-TITLE", "SONG-WRITER", "SONG-CODE", "ISWC",
                                  "SHARE-PERCENT", "USAGE", "INCOME-TYPE", "SOURCE", "DURATION"]),
                   on=["Dist.", "Member Name", "Member Number", "AV Title", "Episode",
                       "Work Title", "Composer(s)/Author(s)", "Work Number", "ISWC",
                       "Share %", "Use", "Source", "Station", "Duration"],
                   how='left', lsuffix="BedTrack", rsuffix="SOCAN")
    # print(df3.dtypes)
    print(df3)

    # Join CSV into DAT
    df4 = df1.join(df2.set_index(["Dist.", "Member Name", "Member Number", "AV Title", "Episode",
                                  "Work Title", "Composer(s)/Author(s)", "Work Number", "ISWC",
                                  "Share %", "Use", "Source", "Station", "Duration"]),
                   on=["DIST.", "SUPPLIER-NAME", "SUPPLIER-NO", "SERIES-TITLE", "AV-WORK-TITLE",
                       "SONG-TITLE", "SONG-WRITER", "SONG-CODE", "ISWC",
                       "SHARE-PERCENT", "USAGE", "INCOME-TYPE", "SOURCE", "DURATION"],
                   how='left', lsuffix="BedTrack", rsuffix="SOCAN")
    # print(df3.dtypes)
    print(df4)

    pd.reset_option("display.max_columns")

