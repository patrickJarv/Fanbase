username = "root"
password = "Dsci-551"
host = "3.145.52.204"
port = 3306
database = "mlbstats22"

import sqlalchemy
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd
from sshtunnel import SSHTunnelForwarder

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import json

from SQL.query import query_sql, query_sql_multi, query_sql_not_multi, query_awards, query_player, query_players
from SQL.insert import insert_award
from SQL.delete import delete_award
from SQL.update import update_award

from NoSQL.code.helper import *


def invalid_syntax():
    print("Invalid syntax. Type 'help' to see proper input")
    print()

def print_help():
    print()
    print('Listed below are all possible commands you can run with Fanbase!')
    print()
    print('\tUser Commands')
    print('\t\tlogin')
    print('\t\tlogout')
    print()
    print('\tQuery:')
    print('\t\tposition_stat [multi/not multi] [WHERE clauses] [group (column name)] [order (column name) asc/desc] [show (column names)]')
    print('\t\tpitcher_stat [multi/not multi] [WHERE clauses] [group (column name)] [order (column name) asc/desc] [show (column names)]')
    print('\t\tawards_stat [WHERE clauses] [show (column names)]')
    print('\t\tshow player_fname player_lname')
    print('\t\tshow set_variable_name')
    print('\t\tusers read')
    print()
    print('\t\tcolumns names = {"metro", "population", "region", "teams"}')
    print('\t\tmetros select [column names]')
    print('\t\tmetros filter [column name (not "teams")] [operator {<=/==/>=/...}] [value]')
    print('\t\tmetros group')
    print('\t\tmetros order [column name (not "teams")] [operator {"asc"/"desc"]')
    print()
    print('\tSet Variable:')
    print('\t\tvar_name = position_stat ...')
    print('\t\tvar_name = pitcher_stat ...')
    print()
    print('\tInsert')
    print('\t\taward insert award_name player_id')
    print('\t\tusers insert [username] [password] [favorite_team]')
    print()
    print('\tUpdate')
    print('\t\taward update old_award_name new_award_name')
    print('\t\tusers update [username] [favorite_team/password]')
    print()
    print('\tDelete')
    print('\t\taward delete award_name player_id')
    print('\t\tusers delete [username]')
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
        by = None
        order = None
        if 'group' in params:
            idx_g = params.index("group")
            by = params[idx_g + 1]
            if "show" in params:
                idx = params.index("show")
                cols = params[idx+1:]
                filters = params[3:idx]
                del params[idx_g + 1]
                del params[idx_g]
            else:
                print("Invalid syntax. See 'help' for correct syntax")
                return
        
        if "show" in params:
            idx = params.index("show")
            cols = params[idx+1:]
            filters = params[1:idx]

        if "order" in params:
            idx = params.index("order")
            if len(params) < idx + 3:
                print("invalid order lsyntax")
                return
            order = params[idx + 1] + " " + params[idx + 2]
            if params[idx + 2] != "desc" and params[idx + 2] != "asc":
                print("invalid order syntax")
                return
            del params[idx + 2]
            del params[idx + 1]
            del params[idx]
            if "order" not in filters:
                print("Invalid ordering of clauses in query. See proper syntax")
                return
            idx_f = filters.index("order")
            del filters[idx_f + 2]
            del filters[idx_f + 1]
            del filters[idx_f]
        
        if len(params) > 1:
            if params[1] == "multi":
                return query_sql_multi(params[0], cols if cols is not None else [], filters[1:], order, by, my_conn)
            elif params[1] == "not" and params[2] == "multi":
                return query_sql_not_multi(params[0], cols if cols is not None else [], filters[2:], order, by, my_conn)
            else:
                return query_sql(params[0], cols if cols is not None else [], filters, order, by, my_conn)
        else:
            return query_sql(params[0], cols if cols is not None else [], filters, order, by, my_conn)

    else:
        return query_sql(params[0], [], [], None, None, my_conn)
    


if __name__ == "__main__":
    user = None
    tunnel = SSHTunnelForwarder(
        host,
        ssh_username="ubuntu",
        ssh_pkey="dsci551_ubuntu.pem",
        remote_bind_address=('127.0.0.1', 3306)
    )
    tunnel.start()
    my_conn = pymysql.connect(host='127.0.0.1', user=username,
                passwd=password, db=database,
                port=tunnel.local_bind_port)

    user_input = {}

    cred = credentials.Certificate("./NoSQL/project-d247d-firebase-adminsdk-hpskj-b7566ae1c5.json")
    default_app = firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://project-d247d-default-rtdb.firebaseio.com/',
    })
    USERS_REF = db.reference('users')
    METROS_REF = db.reference('metros').get()
    
    print()
    print("Welcome to the Fanbase Program!")
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
            username = input()
            print('Password: ', end='')
            pw = input()
            
            # if user exists and pw is correct, login
            if find_user(username, USERS_REF) and pw == USERS_REF.get()[username]['password']:
                print("Login successful!")
                user = username
            else:
                print("Login failed. Please try again.")
        elif response == 'logout':
            if user is None:
                print('You are currently not signed in.')
            else:
                print('Successfully logged out')
                user = None
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
                if params[1] == "user" and params[2] == "favorite":
                    if user is None:
                        print("You must be logged in to perform this task")
                        continue
                    ids = get_favorites(user, USERS_REF)
                    query_players(ids, my_conn)
                else:
                    full_name = params[1] + " " + params[2]
                    query_player(full_name, user, my_conn)

        elif params[0] == 'award':
            if user is None:
                print("You must be logged in to perform this task")
            else:
                if params[1] == 'insert':
                    if len(params) > 3:
                        award = params[2]
                        id = params[3]
                        insert_award(id, award, user, my_conn)
                if params[1] == 'update':
                    if len(params) > 3:
                        old_name = params[2]
                        new_name = params[3]
                        update_award(old_name, new_name, user, my_conn)
                if params[1] == 'delete':
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
                            set_favorites(user, (list(user_input[stored_val][user_input[stored_val].columns[0]])), USERS_REF)
                            print("Successfully set " + stored_val + " as favorite.")
                        else:
                            print(stored_val + " is not a set variable")
        # start of NoSQL ---------------------------------------------------------------                            
        elif params[0] == 'users':
            if params[1] == 'read':
                users_dict = USERS_REF.get()
                for user, data in users_dict.items():
                    print(f'\n{user}:')
                    print(json.dumps(data, indent=4))
            else:
                username = params[2]
                res = find_user(username, USERS_REF)
                if find_user(username, USERS_REF):
                    if params[1] == 'insert': 
                        print('User already exists.')
                    elif params[1] == 'update':
                        favorite_team = params[3]
                        USERS_REF.child(username).update({
                            'favorite_team': favorite_team
                        })
                        print('User updated successfully!')
                    elif params[1] == 'delete':
                        USERS_REF.child(username).delete()
                        print('User deleted successfully!')
                elif not find_user(username, USERS_REF):
                    if params[1] == 'insert':
                        pwd = params[3]
                        favorite_team = params[4]
                        USERS_REF.child(username).set({
                            'password': pwd,
                            'favorite_team': favorite_team
                        })
                        print('User created successfully!')
                    else:
                        output_error(res)
                else: # res == None
                    output_error(res)
        elif params[0] == 'metros':
            print('Using NoSQL to query metros...')
            if params[1] == 'select':
                COLS = params[2:]
                res = [select(obj, COLS) for obj in METROS_REF]
            elif params[1] == 'filter':
                COL = params[2]
                OP = params[3]
                VAL = params[4]

                res = [obj for obj in METROS_REF if compare(obj[COL], OP, VAL)]
            elif params[1] == 'group':
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
            elif params[1] == 'order':
                COL = params[2]
                OP = params[3]
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

        else:
            invalid_syntax()
    
    tunnel.close()