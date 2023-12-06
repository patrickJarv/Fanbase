import pandas as pd

def update_award(old_name, new_name, user, conn):
    res = pd.read_sql(f'SELECT * FROM mlb_awards WHERE award="{old_name}"', conn)
    if len(res) == 0:
        print(f'No Players have the {old_name} award.')
        return
    try:
        sql = f'UPDATE mlb_awards SET award = "{new_name}" WHERE user_id = "{user}" AND award="{old_name}"'
        conn.cursor().execute(sql)
        conn.commit()
        print(f'Award {old_name} has been changed to {new_name}')
    except:
        print(f'Error! Award {new_name} already exists for players')