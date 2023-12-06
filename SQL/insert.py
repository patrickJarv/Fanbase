import pandas as pd

def insert_award(id, award, user, conn):
    res = pd.read_sql(f'SELECT * FROM mlb_players WHERE pid="{id}"', conn)
    if len(res) == 0:
        print("Invalid Player ID")
        return
    
    try:
        sql = f'INSERT INTO mlb_awards (id, award, user_id) VALUES (%s, %s, %s)'
        conn.cursor().execute(sql, (id, award, user))
        conn.commit()
        print(f'Award {award} has been added for {id}')
    except:
        print("Error! Duplicate Primary Key entry")