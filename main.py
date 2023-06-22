"""
Code to analyze SOCAN Royalty statements
"""
import csv
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
from tabulate import tabulate


def datFormat():
    # Adding Format to .dat file and saving to .csv
    datFile = pd.read_fwf("B01_SOCAN_2012toJun2022.dat", header=None, index_col=False,
                          colspecs=[(0, 4), (4, 5), (5, 50), (50, 60), (60, 90),
                                    (90, 113), (113, 117), (117, 132), (132, 138), (138, 148),
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

    # print(dataFrame)
    print(dataFrame.iloc[1])
    print(dataFrame.iloc[27422])
    pd.reset_option("display.max_columns")

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

    pd.set_option("display.max_columns", len(dataFrame))
    # print(dataFrame)
    # print(dataFrame.iloc[1])
    # print(dataFrame.iloc[27422])


    # Rearrange columns
    dataFrame = dataFrame[["DIST.", "DIST-DATE", "PERF-QTR", "DIST-PERIOD-START", "DIST-PERIOD-END", "SPECIAL-DIST",
                           "SUPPLIER-NAME", "SUPPLIER-NO", "IPI NAME NO",
                           "SERIES-NO", "SERIES-TITLE", "AV-WORK-NO", "AV-WORK-TITLE",
                           "SONG-TITLE", "SONG-WRITER", "SONG-CODE",
                           "ISWC", "SHARE-PERCENT", "USAGE", "PERF-COUNT", "SOCIETY", "INCOME-TYPE",
                           "SOURCE", "DURATION", "PERF-DATE", "PERFORMERS", "VENUE", "CITY", "PERF-TYPE",
                           "EARNINGS-AMOUNT"]]
    dataFrame["PERF-QTR"] = dataFrame["PERF-QTR"].replace("", 0)
    dataFrame["PERF-QTR"] = dataFrame["PERF-QTR"].astype(int)
    dataFrame["PERF-QTR"] = dataFrame["PERF-QTR"].replace(0, "")
    print(dataFrame.iloc[0])
    dataFrame.to_csv("./Dat.csv", encoding="utf-8", index=False)
    pd.reset_option("display.max_columns")
    return


def cleanDataFrames(dat="./B01_SOCAN_2012toJun2022_Dat.csv"):
    df1 = pd.DataFrame(pd.read_csv(dat, index_col=False))
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


def compare(df1, df2):
    df_comparison = pd.concat([df2, df1], axis=1)
    df_comparison["Difference"] = np.nan
    df_comparison.loc[df_comparison["Use"] != df_comparison["USAGE"], "Difference"] = "Use&USAGE"
    df_comparison.loc[df_comparison["Source"] != df_comparison["INCOME-TYPE"], "Difference"] = "Source&INCOME-TYPE"
    df_comparison.loc[df_comparison["Duration"] != df_comparison["DURATION"], "Difference"] = "Duration&DURATION"
    df = pd.concat([df_comparison[df_comparison["Use"] != df_comparison["USAGE"]],
                    df_comparison[df_comparison["Source"] != df_comparison["INCOME-TYPE"]],
                    df_comparison[df_comparison["Duration"] != df_comparison["DURATION"]]])
    print(df["Difference"])
    # df = df_comparison[df_comparison["Use"] != df_comparison["USAGE"]]
    df.to_csv("./FilesComparison.csv", encoding="utf-8", index=False)

    print(df1[["DIST.", "SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE", "DURATION",
               "EARNINGS-AMOUNT", "ISWC", "SHARE-PERCENT"]])
    print(df2[["Dist.", "Work Number", "Use", "Station", "Source", "Duration",
               "Amount", "ISWC", "Share %"]])
    print(df1.groupby(["DIST.", "SONG-CODE", "USAGE", "SOURCE", "INCOME-TYPE", "DURATION",
                       "EARNINGS-AMOUNT", "SHARE-PERCENT"]).size())
    print(df2.groupby(["Dist.", "Work Number", "Use", "Station", "Source", "Duration",
                       "Amount", "Share %"]).size())

    return


def cleanFile(file):
    # CLean file of music album reviews
    # print(file["Genre"])
    # print(file["Genre"].str.contains("pop", na=False, regex=True))
    # print(file["Genre"][file["Genre"].str.contains("pop", na=False, regex=True)])

    print(file["Genre"].nunique())
    # Change Genre
    file.loc[file["Genre"].str.contains("Pop", na=False, regex=True), "Genre"] = "Pop"
    file.loc[file["Genre"].str.contains("pop", na=False, regex=True), "Genre"] = "Pop"

    file.loc[file["Genre"].str.contains("Rock", na=False, regex=True), "Genre"] = "Rock"
    file.loc[file["Genre"].str.contains("Punk", na=False, regex=True), "Genre"] = "Punk"
    file.loc[file["Genre"].str.contains("Hip Hop", na=False, regex=True), "Genre"] = "Hip Hop"
    file.loc[file["Genre"].str.contains("Rap", na=False, regex=True), "Genre"] = "Rap"
    file.loc[file["Genre"].str.contains("Jazz", na=False, regex=True), "Genre"] = "Jazz"
    file.loc[file["Genre"].str.contains("Funk", na=False, regex=True), "Genre"] = "Funk"
    file.loc[file["Genre"].str.contains("Folk", na=False, regex=True), "Genre"] = "Folk"
    file.loc[file["Genre"].str.contains("Alternative", na=False, regex=True), "Genre"] = "Alternative"
    file.loc[file["Genre"].str.contains("Metal", na=False, regex=True), "Genre"] = "Metal"
    file.loc[file["Genre"].str.contains("Country", na=False, regex=True), "Genre"] = "Country"
    file.loc[file["Genre"].str.contains("Ambient", na=False, regex=True), "Genre"] = "Ambient"
    file.loc[file["Genre"].str.contains("Electro", na=False, regex=True), "Genre"] = "Electro"
    file.loc[file["Genre"].str.contains("Techno", na=False, regex=True), "Genre"] = "Techno"
    file.loc[file["Genre"].str.contains("Minimal", na=False, regex=True), "Genre"] = "Minimal"
    file.loc[file["Genre"].str.contains("Psychedelic", na=False, regex=True), "Genre"] = "Psychedelic"
    file.loc[file["Genre"].str.contains("Progressive", na=False, regex=True), "Genre"] = "Progressive"
    file.loc[file["Genre"].str.contains("Blue", na=False, regex=True), "Genre"] = "Blues"

    print(file["Genre"].nunique())
    # print(file["Release Year"].between(1940, 1949, inclusive=True))
    
    file.loc[file["Release Year"].between(1940, 1949, inclusive=True), "Decade"] = "40s"
    file.loc[file["Release Year"].between(1950, 1959, inclusive=True), "Decade"] = "50s"
    file.loc[file["Release Year"].between(1960, 1969, inclusive=True), "Decade"] = "60s"
    file.loc[file["Release Year"].between(1970, 1979, inclusive=True), "Decade"] = "70s"
    file.loc[file["Release Year"].between(1980, 1989, inclusive=True), "Decade"] = "80s"
    file.loc[file["Release Year"].between(1990, 1999, inclusive=True), "Decade"] = "90s"
    file.loc[file["Release Year"].between(2000, 2009, inclusive=True), "Decade"] = "2000s"
    file.loc[file["Release Year"].between(2010, 2020, inclusive=True), "Decade"] = "2010s"

    # print(file["Decade"])
    file.to_csv("~/Documents/BedTracks/archive/album_ratings2.csv", encoding="utf-8", index=False)

    return


if __name__ == '__main__':
    # datFormat()
    df1 = pd.DataFrame(pd.read_csv("~/Documents/BedTracks/archive/album_ratings.csv"))
    pd.set_option("display.max_columns", len(df1))
    # print(df1)
    cleanFile(df1)
    pd.reset_option("display.max_columns")

