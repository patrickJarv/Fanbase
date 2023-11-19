username = "root"
password = "PbjMySQL123"
host = "localhost"
port = 3306
database = "mlbstats22"

import sqlalchemy
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd

from SQL.query import query_sql, query_sql_multi, query_sql_not_multi, query_awards, query_player
from SQL.insert import insert_award
from SQL.delete import delete_award
from SQL.update import update_award


def invalid_syntax():
    print("Invalid syntax. Type 'help' to see proper input")
    print()

def print_help():
    print()
    print('Listed below are all possible commands you can run with Fanbase!')
    print()
    print('\tUser Commands')
    print('\t\tlogin')
    print('\t\tsignup')
    print('\t\tlogout')
    print()
    print('\tQuery:')
    print('\t\tposition_stat [multi/not multi] [WHERE clauses] [show (column names)]')
    print('\t\tpitcher_stat [multi/not multi] [WHERE clauses] [show (column names)]')
    print('\t\tawards_stat [WHERE clauses] [show (column names)]')
    print('\t\tshow player_fname player_lname')
    print('\t\tshow set_variable_name')
    print()
    print('\tSet Variable:')
    print('\t\tvar_name = position_stat ...')
    print('\t\tvar_name = pitcher_stat ...')
    print()
    print('\tInsert')
    print('\t\tinsert award award_name player_id')
    print()
    print('\tUpdate')
    print('\t\tupdate award old_award_name new_award_name')
    print()
    print('\tDelete')
    print('\t\tdelete award award_name player_id')
    print()
    print('\tOther commands')
    print('\t\tquit')
    print('\t\thelp')
    print()
    print('Please also refer to the provided docs for better detail of each command')
    print()

def evaluate_query(params):
    cols = None
    filters = []
    if len(params) > 1:
        filters = params[1:]
        if "show" in params:
            idx = params.index("show")
            cols = params[idx+1:]
            filters = params[1:idx]
        
        if params[1] == "multi":
            return query_sql_multi(params[0], cols if cols is not None else [], filters[1:], my_conn)
        elif params[1] == "not" and params[2] == "multi":
            return query_sql_not_multi(params[0], cols if cols is not None else [], filters[2:], my_conn)
        else:
            return query_sql(params[0], cols if cols is not None else [], filters, my_conn)

    else:
        return query_sql(params[0], [], [], my_conn)

if __name__ == "__main__":
    user = "test"
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
        if len(params) == 0:
            continue
        if response == 'quit':
            break
        elif response == 'help':
            print_help()
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
            resp = evaluate_query(params)
            if resp is not None:
                print(resp)

        elif params[0] == 'awards_stat':
            cols = None
            filters = None
            if len(params) > 1:
                filters = params[1:]
                if "show" in params:
                    idx = params.index("show")
                    cols = params[idx+1:]
                    filters = params[1:idx]
            query_awards(cols, filters, user, my_conn)

        elif params[0] == 'show':
            if len(params) == 2:
                id = params[1]
                if id not in user_input:
                    print("Variable " + id + " is not registered.")
                else:
                    print(user_input[id])
            if len(params) == 3:
                full_name = params[1] + " " + params[2]
                query_player(full_name, user, my_conn)

        elif params[0] == 'insert':
            if user is None:
                print("You must be logged in to perform this task")
            else:
                if params[1] == 'award':
                    if len(params) > 3:
                        award = params[2]
                        id = params[3]
                        insert_award(id, award, user, my_conn)
        elif params[0] == 'update':
            if user is None:
                print("You must be logged in to perform this task")
            else:
                if params[1] == 'award':
                    if len(params) > 3:
                        old_name = params[2]
                        new_name = params[3]
                        update_award(old_name, new_name, user, my_conn)
        elif params[0] == 'delete':
            if user is None:
                print("You must be logged in to perform this task")
            else:
                if params[1] == 'award':
                    if len(params) > 3:
                        award = params[2]
                        id = params[3]
                        delete_award(id, award, user, my_conn)
        elif params[0] == 'register':
            if user is None:
                print("You must be logged in to perform this task")
            else:
                if params[1] == 'favorite':
                    if len(params) == 3:
                        stored_val = params[2]
                        if stored_val in user_input:
                            # NoSQL insert list of IDs as favorite
                            continue
        elif len(params) > 1:
            if params[1] == "=":
                var_name = params[0]
                resp = evaluate_query(params[2:])
                if resp is not None:
                    user_input[var_name] = resp
            else:
                invalid_syntax()

        else:
            invalid_syntax()
