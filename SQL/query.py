import pandas as pd

position_cols = "t.Id, t.Name, t.Age, t.team, t.league, t.G, t.PA, t.AB, t.R, t.H, t.2B, t.3B, t.HR, t.RBI, t.SB, t.CS, t.BB, t.SO, t.BA, t.OBP, t.SLG, t.OPS, t.OPSp, t.TB, t.GDP, t.HBP, t.SH, t.SF, t.IBB"
pitcher_cols = "t.id, t.name, t.age, t.team, t.league, t.W, t.L, t.WLperc, t.ERA, t.G, t.GS, t.GF, t.CG, t.SHO, t.SV, t.IP, t.H, t.R, t.ER, t.HR, t.BB, t.IBB, t.SO, t.HBP, t.BK, t.WP, t.BF, t.ERAp, t.FIP, t.WHIP, t.H9, t.HR9, t.BB9, t.SO9, t.SO/W"

def query_sql(db, cols, filter, conn, join_statement=""):
    db_name = None
    default_cols = None
    if db == 'position_stat':
        db_name = "mlb_position"
        default_cols = position_cols
    if db == 'pitcher_stat':
        # df = pd.read_sql('SELECT * FROM mlb_pitcher', conn)
        db_name = "mlb_pitcher"
        default_cols = pitcher_cols
    
    if db_name == None:
        print("Invalid syntax. Use 'help' to find proper syntax")
        return None

    if "award" in filter:
        no_award = ""
        award_idx = filter.index("award")
        if award_idx > 0:
            if filter[award_idx-1] == "no":
                no_award = "NOT "
                del filter[award_idx-1]
                award_idx = award_idx -1
        filter[award_idx] = f'id {no_award}IN (SELECT id FROM mlb_awards)'


    filter_str = " ".join(filter)
    columns = default_cols
    if len(cols) > 0:
        columns = ", ".join(cols)

    sql_string = f'SELECT {columns} FROM {db_name} as t'
    if len(filter) > 0:
        sql_string = f'SELECT {columns} FROM {db_name} as t {join_statement}WHERE {filter_str}'
    print(sql_string)
    try:
        df = pd.read_sql(sql_string, conn)
        return df
    except:
        print("Invalid filter syntax")
        print()
        return None

def query_sql_multi(db, cols, filter, conn):
    join_statement = "JOIN mlb_players as a ON t.id = a.pid "
    if len(filter) > 0:
        filter.insert(0, "AND")
    filter.insert(0, "a.team2 <> ''")
    return query_sql(db, cols, filter, conn, join_statement)

def query_sql_not_multi(db, cols, filter, conn):
    join_statement = "JOIN mlb_players as a ON t.id = a.pid "
    if len(filter) > 0:
        filter.insert(0, "AND")
    filter.insert(0, "a.team2 = ''")
    return query_sql(db, cols, filter, conn, join_statement)

def query_awards(cols, filters, user, conn):
    default_cols = "id, award, user_id"
    columns = ", ".join(cols) if cols is not None else default_cols

    user_select = "user_id = 'MLB'"
    if user is not None:
        user_select = f'(user_id = "MLB" or user_id = "{user}")'

    sql_string = f'SELECT {columns} FROM mlb_awards WHERE {user_select}'
    if filters is not None and len(filters) > 0:
        filter_str = " ".join(filters)
        sql_string = f'SELECT {columns} FROM mlb_awards WHERE {user_select} AND {filter_str}'
    print(sql_string)
    try:
        df = pd.read_sql(sql_string, conn)
        print(df)
    except:
        print("Invalid filter syntax")