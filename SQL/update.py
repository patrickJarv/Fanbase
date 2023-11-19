from sqlalchemy import update
import pandas as pd

from sqlalchemy import Table, Column, VARCHAR, MetaData
metadata_obj = MetaData()
award_table = Table("mlb_awards", metadata_obj, Column("id", VARCHAR(40), primary_key=True), Column("award", VARCHAR(40)),Column("user_id", VARCHAR(40)))

def update_award(old_name, new_name, user, conn):
    res = pd.read_sql(f'SELECT * FROM mlb_awards WHERE award="{old_name}"', conn)
    if len(res) == 0:
        print(f'No Players have the {old_name} award.')
        return
    stmt = update(award_table).where(award_table.c.award==old_name, award_table.c.user_id == user).values(award=new_name)
    try:
        conn.execute(stmt)
        print(f'Award {old_name} has been changed to {new_name}')
    except:
        print(f'Error! Award {new_name} already exists for players')