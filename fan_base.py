username = "root"
password = "PbjMySQL123"
host = "localhost"
port = 3306
database = "mlbstats22"

import sqlalchemy
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd

from SQL.query import query_sql, query_sql_multi, query_sql_not_multi


if __name__ == "__main__":
    user = None
    my_conn = sqlalchemy.create_engine(f'mysql+mysqldb://{username}:{password}@{host}:{port}/{database}')
    user_input = {}
    
    print()
    print("Welcome to the Fan Bases Program!")
    print("To begin, please 'login' or 'signup'")
    print("Or ask for 'help'")
    print("When finished, type 'quit'")
    print()

    while True:
        print("fb> ", end='')
        response = input()
        params = response.split()

        if response == 'quit':
            break
        elif response == 'help':
            print("IDK")
        elif response == 'login':
            print("Username: ", end='')
            user = input()
            print('Password: ', end='')
            pw = input()
            # validate using NoSQL
        elif response == 'signup':
            print("Username: ", end='')
            user = input()
            # validate that username isn't already taken
            print('Password: ', end='')
            pw = input()
            # input into NoSQL
        elif params[0] == 'position_stat' or params[0] == 'pitcher_stat':
            if params[1] == "multi":
                query_sql_multi(params[0], params[2:], my_conn)
            elif params[1] == "not" and params[2] == "multi":
                query_sql_not_multi(params[0], params[3:], my_conn)
            else:
                query_sql(params[0], params[1:], my_conn)
