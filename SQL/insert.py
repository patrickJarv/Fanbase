from sqlalchemy import insert
import pandas as pd

from sqlalchemy import Table, Column, VARCHAR, MetaData
metadata_obj = MetaData()
award_table = Table("mlb_awards", metadata_obj, Column("id", VARCHAR(40), primary_key=True), Column("award", VARCHAR(40)),Column("user_id", VARCHAR(40)))

def insert_award(id, award, user, conn):
    res = pd.read_sql(f'SELECT * FROM mlb_players WHERE pid="{id}"', conn)
    if len(res) == 0:
        print("Invalid Player ID")
        return
    stmt = insert(award_table).values(id=id, award=award, user_id = user)
    try:
        conn.execute(stmt)
        print(f'Award {award} has been added for {id}')
    except:
        print("Error! Duplicate Primary Key entry")