#!/usr/bin/env python

import requests
import json
import psycopg2
from datetime import date
from array import array


def main():
    listOfIntsPageViews = []
    listOfIntsVisits = []
    listOfDepth = []
    response = requests.get("свой запрос к API ym")
    content = response.content
    json_string = json.loads(content)
    print(json_string)

    visits = json_string["data"][0]["metrics"][0]
    pageviews = json_string["data"][0]["metrics"][1]
    date1=json_string["query"]["date1"]
    date2=json_string["query"]["date2"]



    print('\nVISITS\n*** *** *** *** *** *** *** \n')
    for item2 in visits:
        listOfIntsVisits.append((round(item2)))
    print(listOfIntsVisits)


    print('\nPAGEVIEWS\n*** *** *** *** *** *** *** \n')
    for item in pageviews:
        listOfIntsPageViews.append((round(item)))
    print(listOfIntsPageViews)

    print('\nDEPTH\n*** *** *** *** *** *** *** \n')
    for x in range(28):
        if listOfIntsVisits[x] != 0:
            listOfDepth.append(round((listOfIntsPageViews[x]/listOfIntsVisits[x]), 2))
        else:
            listOfDepth.append(0)
    print(listOfDepth)

    print('\nDATES\n*** *** *** *** *** *** *** \n')
    print(date1, ' ', date2)

    #Подключаемся к БД
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="test_database")
        cursor = connection.cursor()
        print(connection.get_dsn_parameters(), "\n")
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

        cursor.execute("""INSERT INTO yandex_metrics(date)
           SELECT i::date from generate_series(%s, %s, '1 day'::interval) i;
        """, (date1, date2))
        query_string = 'INSERT INTO yandex_metrics VALUES (%s);' % listOfIntsVisits
        cursor.execute(query_string, listOfIntsVisits)

        cursor.execute("""SELECT * FROM yandex_metrics""")
        for table in cursor.fetchall():
            print(table)

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
    main()
