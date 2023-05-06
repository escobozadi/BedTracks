"""
Code to modify SOCAN Publisher File into a table according to the layout
"""
import csv
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np


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


if __name__ == '__main__':
    # create_connection("localhost", "root", "Dakota.2486Nala")

    # datFile = pd.read_table("B01_SOCAN_2012toJun2022.dat")
    datFile = pd.read_fwf("B01_SOCAN_2012toJun2022.dat", header=None, index_col=False,
                          colspecs=[(0, 4), (4, 5), (5, 50), (50, 60), (60, 90), (90, 113), (113, 117)],
                          names=["DIST-YEAR", "DIST-QTR", "SUPPLIER-NAME", "SONG-CODE", "SONG-TITLE", "SONG-WRITER",
                                 "INCOME-TYPE"])
    print(datFile)
