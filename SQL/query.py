import pandas as pd

position_cols = "t.Id, t.Name, t.Age, t.Tm, t.Lg, t.G, t.PA, t.AB, t.R, t.H, t.2B, t.3B, t.HR, t.RBI, t.SB, t.CS, t.BB, t.SO, t.BA, t.OBP, t.SLG, t.OPS, t.OPS+, t.TB, t.GDP, t.HBP, t.SH, t.SF, t.IBB"
pitcher_cols = "t.id, t.name, t.age, t.team, t.league, t.W, t.L, t.W-L%, t.ERA, t.G, t.GS, t.GF, t.CG, t.SHO, t.SV, t.IP, t.H, t.R, t.ER, t.HR, t.BB, t.IBB, t.SO, t.HBP, t.BK, t.WP, t.BF, t.ERA+, t.FIP, t.WHIP, t.H9, t.HR9, t.BB9, t.SO9, t.SO/W"

def query_sql(db, filter, conn):
    db_name = None
    cols = None
    if db == 'position_stat':
        db_name = "mlb_position"
        cols = position_cols
    if db == 'pitcher_stat':
        # df = pd.read_sql('SELECT * FROM mlb_pitcher', conn)
        db_name = "mlb_pitcher"
        cols = pitcher_cols
    
    if db_name == None:
        print("Invalid syntax. Use 'help' to find proper syntax")

    filter_str = " ".join(filter)
    
    sql_string = f'SELECT {cols} FROM {db_name} as t {filter_str}'
    print(sql_string)
    try:
        df = pd.read_sql(sql_string, conn)
        print(df)
    except:
        print("Invalid filter syntax")

def query_sql_multi(db, filter, conn):
    join_statement = "JOIN mlb_awards as a ON t.id = a.id"
    filter.append("AND")
    filter.append("a.team2 <> ''")
    filter.append(join_statement)
    query_sql(db, filter, conn)

def query_sql_not_multi(db, filter, conn):
    join_statement = "JOIN mlb_awards as a ON t.id = a.id"
    filter.append("AND")
    filter.append("a.team2 = ''")
    filter.append(join_statement)
    query_sql(db, filter, conn)