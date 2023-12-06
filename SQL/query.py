import pandas as pd
from sqlalchemy import text

position_cols = "t.Id, t.Name, t.Age, t.team, t.league, t.G, t.PA, t.AB, t.R, t.H, t.2B, t.3B, t.HR, t.RBI, t.SB, t.CS, t.BB, t.SO, t.BA, t.OBP, t.SLG, t.OPS, t.OPSp, t.TB, t.GDP, t.HBP, t.SH, t.SF, t.IBB"
pitcher_cols = "t.id, t.name, t.age, t.team, t.league, t.W, t.L, t.WLperc, t.ERA, t.G, t.GS, t.GF, t.CG, t.SHO, t.SV, t.IP, t.H, t.R, t.ER, t.HR, t.BB, t.IBB, t.SO, t.HBP, t.BK, t.WP, t.BF, t.ERAp, t.FIP, t.WHIP, t.H9, t.HR9, t.BB9, t.SO9, t.SO/W"

def query_sql(db, cols, filter, order, group, conn, join_statement=""):
    db_name = None
    default_cols = None
    if db == 'position_stat':
        db_name = "mlb_position"
        default_cols = position_cols
    if db == 'pitcher_stat':
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
    if group is not None:
        sql_string = sql_string + " GROUP BY " + group
    if order is not None:
        sql_string = sql_string + " ORDER BY " + order

    sql_string = sql_string + " LIMIT 1000"
    try:
        df = pd.read_sql(sql_string, conn)
        return df
    except:
        print("Invalid filter syntax")
        print()
        return None

def query_sql_multi(db, cols, filter, order, group, conn):
    join_statement = "JOIN mlb_players as a ON t.id = a.pid "
    if len(filter) > 0:
        filter.insert(0, "AND")
    filter.insert(0, "a.team2 <> ''")
    return query_sql(db, cols, filter, order, group, conn, join_statement)

def query_sql_not_multi(db, cols, filter, order, group, conn):
    join_statement = "JOIN mlb_players as a ON t.id = a.pid "
    if len(filter) > 0:
        filter.insert(0, "AND")
    filter.insert(0, "a.team2 = ''")
    return query_sql(db, cols, filter, order, group, conn, join_statement)

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

    try:
        df = pd.read_sql(sql_string, conn)
        print(df)
    except:
        print("Invalid filter syntax")

def query_player(full_name, user, conn):
    names = full_name.split()
    id_pattern = names[0].lower() + "_" + names[1].lower() + "%"
    sql_string = "SELECT * FROM mlb_players WHERE pid LIKE \"" + id_pattern + "\""
    df = pd.read_sql(sql_string, conn)
    if len(df) == 0:
        print(full_name + " is not a registered player.")
    else:
        for i in range(len(df)):
            player_val = df.iloc[i].to_dict()
            player_id = player_val['pid']
            teams = [player_val['team1']]
            player_type = "Pitcher" if player_val['pitcher'] == 1 else "Position"


            if player_val['team2'] != '':
                teams.append(player_val['team2'])
            if player_val['team3'] != '':
                teams.append(player_val['team3'])
            if player_val['team4'] != '':
                teams.append(player_val['team4'])

            stat_df = None
            if player_type == "Pitcher":
                sql_string = f'SELECT * FROM mlb_pitcher WHERE id="{player_id}"'
                stat_df = pd.read_sql(sql_string, conn)
            else:
                sql_string = f'SELECT * FROM mlb_position WHERE id="{player_id}"'
                stat_df = pd.read_sql(sql_string, conn)

            user_query = "(user_id = \"MLB\")"
            if user is not None:
                user_query = f'(user_id = "MLB" or user_id="{user}")'

            award_df = pd.read_sql(f'SELECT * FROM mlb_awards WHERE id="{player_id}" AND {user_query}', conn)
            awards = "None"
            if len(award_df) > 0:
                awards = ", ".join(list(award_df['award']))
            print("----------------------------------------------------------")
            print(full_name + " - " + player_type)
            print("Teams: " +", ".join(teams))
            print("Player Stats: ")
            print(stat_df)
            print('Awards: ' + awards)
            print("----------------------------------------------------------")
            print()
            print()

def query_players(ids, conn):
    sql_statement = "SELECT * FROM mlb_position WHERE id IN (" + str(ids)[1:-1] + ")"
    pos_df = pd.read_sql(sql_statement, conn)
    if len(pos_df) > 0:
        print(pos_df)
    sql_statement = "SELECT * FROM mlb_pitcher WHERE id IN (" + str(ids)[1:-1] + ")"
    pitch_df = pd.read_sql(sql_statement, conn)
    if len(pitch_df) > 0:
        print(pitch_df)

