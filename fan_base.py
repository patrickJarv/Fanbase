username = "root"
password = "PbjMySQL123"
host = "localhost"
port = 3306
database = "mlbstats22"

import sqlalchemy
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import json

from SQL.query import query_sql, query_sql_multi, query_sql_not_multi, query_awards
from SQL.insert import insert_award
from SQL.delete import delete_award
from SQL.update import update_award

from NoSQL.code.helper import *


def invalid_syntax():
    print("Invalid syntax. Type 'help' to see proper input")
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

    cred = credentials.Certificate("Fanbase/NoSQL/project-d247d-firebase-adminsdk-hpskj-b7566ae1c5.json")
    default_app = firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://project-d247d-default-rtdb.firebaseio.com/',
    })
    USERS_REF = db.reference('users')
    METROS_REF = db.reference('metros').get()
    print(METROS_REF[0])
    
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
            resp = evaluate_query(params)
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
            if len(params) > 1:
                id = params[1]
                if id not in user_input:
                    print("Variable " + id + " is not registered.")
                else:
                    print(user_input[id])
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
        # start of NoSQL ---------------------------------------------------------------                            
        elif params[0] == 'nosql':
            if params[1] == 'users':
                if params[2] == 'read':
                    users_dict = USERS_REF.get()
                    for user, data in users_dict.items():
                        print(f'\n{user}:')
                        print(json.dumps(data, indent=4))
                else:
                    username = params[3]
                    res = find_user(username, USERS_REF)
                    if find_user(username, USERS_REF):
                        if params[2] == 'insert': 
                            print('User already exists.')
                        elif params[2] == 'update':
                            favorite_team = params[4]
                            USERS_REF.child(username).update({
                                'favorite_team': favorite_team
                            })
                            print('User updated successfully!')
                        elif params[2] == 'delete':
                            USERS_REF.child(username).delete()
                            print('User deleted successfully!')
                    elif not find_user(username, USERS_REF):
                        if params[2] == 'insert':
                            pwd = params[4]
                            favorite_team = params[5]
                            USERS_REF.child(username).set({
                                'password': pwd,
                                'favorite_team': favorite_team
                            })
                            print('User created successfully!')
                        else:
                            output_error(res)
                    else: # res == None
                        output_error(res)
            elif params[1] == 'metros':
                print('Using NoSQL to query metros...')
                if params[2] == 'select':
                    COLS = params[3:]
                    res = [select(obj, COLS) for obj in METROS_REF]
                elif params[2] == 'filter':
                    COL = params[3]
                    OP = params[4]
                    VAL = params[5]

                    res = [obj for obj in METROS_REF if compare(obj[COL], OP, VAL)]
                elif params[2] == 'group':
                    COL = 'region'
                    AGGS = ['count', 'sum', 'avg', 'min', 'max']
                    AGG_COL = 'population'  

                    groups = {}
                    for obj in METROS_REF:
                        if obj[COL] in groups:
                            groups[obj[COL]].append(obj)
                        else:
                            groups[obj[COL]] = [obj]

                    aggs = []
                    for region_group, metro in groups.items():
                        obj = {}
                        obj[COL] = region_group

                        obj['min'], obj['max'] = float('inf'), float('-inf')
                        obj['sum'] = 0
                        for metro_area in metro:
                            val = metro_area[AGG_COL]
                            obj['min'] = min(val, obj['min'])
                            obj['max'] = max(val, obj['max'])
                            obj['sum'] += val
                        obj['count'] = len(metro)
                        obj['avg'] = obj['sum'] / len(metro)

                        aggs.append(obj)
                    pprint(aggs)
                    continue
                elif params[2] == 'order':
                    COL = params[3]
                    OP = params[4]
                    a = [(obj[COL], obj) for obj in METROS_REF]
                    res = order(a, OP)
                else:
                    output_error('Invalid syntax.')
                    continue
                for obj in res:
                    print(json.dumps(obj, indent=4))
        elif len(params) > 1:
            if params[1] == "=":
                var_name = params[0]
                resp = evaluate_query(params[2:])
                if resp is not None:
                    user_input[var_name] = resp
        else:
            invalid_syntax()
