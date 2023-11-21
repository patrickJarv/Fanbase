import numpy as np
import pandas as pd

# Functions
def find_user(username, col):
    # Returns None if collection doesn't exist, True if user exists, False if user doesn't exist
    return None if col.get() is None else username in set(col.get().keys())

def output_error(is_found):
    # Prints error based on find_user output
    if is_found is None:
        print("Collection doesn't exist.")
    elif is_found:
        print("User already exists.")
    else:
        print("User not found.")

def compare(a, op, b):
    if type(a) != type(b) and (type(a) == str or type(a) == int):
        b = type(a)(b)
    elif type(a) == list:
        return np.any(np.array(a) == b)
    
    if op == '==':
        return a == b
    elif op == '!=':
        return a != b
    elif op == '>':
        return a > b
    elif op == '>=':
        return a >= b
    elif op == '<':
        return a < b
    elif op == '<=':
        return a <= b
    return False

def select(obj, cols):
    obj_data = {}
    for col in cols:
        if col in obj.keys():
            obj_data[col] = obj[col]
    return obj_data

def pprint(data):
    print(pd.DataFrame(data).head())

def order(a, op):
    sorted_tuples = sorted(a, key=lambda x: x[0])
    second_elements = [elem[1] for elem in sorted_tuples]
    if op == 'desc':
        return second_elements[::-1]
    elif op == 'asc':
        return second_elements
    
def set_favorites(username, favorites, USERS_REF):
    USERS_REF.child(username).update({
        'favorites': favorites
    })


def get_favorites(username, USERS_REF):
    return USERS_REF.get()[username]['favorites']