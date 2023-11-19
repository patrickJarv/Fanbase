from sqlalchemy import delete
import pandas as pd

from sqlalchemy import Table, Column, VARCHAR, MetaData
metadata_obj = MetaData()
award_table = Table("mlb_awards", metadata_obj, Column("id", VARCHAR(40), primary_key=True), Column("award", VARCHAR(40)),Column("user_id", VARCHAR(40)))

def delete_award(id, award, user, conn):
    res = pd.read_sql(f'SELECT * FROM mlb_awards WHERE id="{id}" AND award="{award}" AND user_id="{user}"', conn)
    if len(res) == 0:
        print(f'User {user} has no record of offering this award to this player')
        return

    stmt = delete(award_table).where(award_table.c.id==id, award_table.c.award==award, award_table.c.user_id == user)
    try:
        resp = conn.execute(stmt)
        print(f'Award {award} has been deleted for {id}')
    except:
        print("Error! Duplicate Primary Key entry")