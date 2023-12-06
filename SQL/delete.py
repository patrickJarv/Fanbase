import pandas as pd

def delete_award(id, award, user, conn):
    res = pd.read_sql(f'SELECT * FROM mlb_awards WHERE id="{id}" AND award="{award}" AND user_id="{user}"', conn)
    if len(res) == 0:
        print(f'User {user} has no record of offering this award to this player')
        return

    sql = f'DELETE FROM mlb_awards WHERE user_id="{user}" and id="{id}" and award="{award}"'
    try:
        conn.cursor().execute(sql)
        conn.commit()
        print(f'Award {award} has been deleted for {id}')
    except:
        print("Error! Duplicate Primary Key entry")