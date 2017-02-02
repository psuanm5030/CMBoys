# Purpose: Simply a common script to centralize commonly used functions

### URL Functions
    # Go here to see how to setup the URLs (resources and sub-resources)
    # https://developer.yahoo.com/fantasysports/guide/#league-resource
def URL_make_league_code(gameid, leagueid):
    return str(gameid) + '.l.' + str(leagueid)

def URL_make_team_code(gameid, leagueid, teamid):
    return str(gameid) + '.l.' + str(leagueid) + '.t.' + str(teamid)

def URL_league_data(league_code):
    return "http://fantasysports.yahooapis.com/fantasy/v2/league/" + league_code

def URL_league_scoreboard(league_code, wk):
    return "http://fantasysports.yahooapis.com/fantasy/v2/league/" + league_code + \
           "/scoreboard;week=" + str(wk)

def URL_week_roster(team_code, wk):
    return "http://fantasysports.yahooapis.com/fantasy/v2/team/" + team_code + "/roster;week=" + str(wk)

def URL_draft_results(league_code):
    return "http://fantasysports.yahooapis.com/fantasy/v2/league/" + league_code + "/draftresults/"

def URL_settings(league_code):
    return "http://fantasysports.yahooapis.com/fantasy/v2/league/" + league_code + "/settings"

def URL_players(league_code):
    return "http://fantasysports.yahooapis.com/fantasy/v2/league/" + league_code + "/players/"

def URL_transactions(league_code):
    return "http://fantasysports.yahooapis.com/fantasy/v2/league/" + league_code + "/transactions/"

def URL_team_data(team_code):
    return "http://fantasysports.yahooapis.com/fantasy/v2/team/" + team_code

def URL_roster_data(team_code, date_wanted):
    return "http://fantasysports.yahooapis.com/fantasy/v2/team/" + team_code + "/roster;date=" + date_wanted.isoformat()

def URL_player_metadata(league_code, start):
    return "http://fantasysports.yahooapis.com/fantasy/v2/league/" + league_code + "/players" + ';start=' + str(start)

def URL_player_stats(league_code, player_key):
    # http://fantasysports.yahooapis.com/fantasy/v2/league/223.l.431/players;player_keys=223.p.5479/stats
    return "http://fantasysports.yahooapis.com/fantasy/v2/league/" + league_code + "/players" + ';player_keys=' + player_key + '/stats'

### Database Functions
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import pandas as pd
import settings

def db_connect():
    """ Make connection to an SQLite database file """
    return create_engine(URL(**settings.DATABASE), isolation_level="AUTOCOMMIT")

def db_close(conn):
    """ Commit changes and close connection to the database """
    # conn.commit()
    conn.close()

def db_setup_new(table, data, engine):
    """
    This adds the specified dataframe to the database (replaces the current table).
    :param table: table name for database
    :param data: dataframe for consumption
    :return: none
    """
    pre_checks(data) # Perform pre-checks
    # Send data to DB
    data.to_sql(table, engine, if_exists='replace', index=False)
    # Make Primary Key
    stmt = 'ALTER TABLE public.' + table + ' ADD CONSTRAINT ' + table + '_pkey PRIMARY KEY ("Primary_Key");'
    with engine.connect() as con:
        con.execute(stmt)
    rows_imported = data.__len__()
    print 'Imported (drop table and import) ' + str(rows_imported) + ' rows into: ' + table + ' - database table. Also, setup primary key.'

def db_add_rows(table, data, engine):
    """
    This does not "replace" the table, but adds any rows that have a unique Primary_Key that is not already in the database.
    :param table: table name for database
    :param data: dataframe for consumption
    :return: none
    """
    pre_checks(data) # Perform pre-checks
    cnt = 0
    for i in range(len(data)):
        try:
            data.iloc[i:i + 1].to_sql(name=table, con=engine, if_exists='append', index=False)
            print 'Added row: ' +  data.iloc[i:i + 1]['Primary_Key']
            cnt += 1
        except:
            # print 'Passed on: ' +  data.iloc[i:i + 1]['Primary_Key']
            pass  # or any other action

    print 'Appended ' + str(cnt) + ' rows to ' + table + ' database table.'

def pre_checks(data):
    """
    Checks to make sure the incoming data is a dataframe and has a field called "Primary_Key".
    :param data: dataframe
    :return: none
    """
    if type(data) != pd.core.frame.DataFrame:
        raise Exception('Data is NOT a dataframe.  Please try again with a data frame.')
    if 'Primary_Key' not in data.columns:
        raise Exception('Missing Primary_Key field in data.  This is required for the database.  Please add a field entitled Primary_Key and try again.')

### Seasons / League & Game ID Creation
def getGameID():
    '''
    Use this function to return the game_id for the current year of NFL.
    :return:
    '''
    import urllib2
    baseURL = "http://fantasysports.yahooapis.com/fantasy/v2/game/nfl"
    content = Master_Auth.api_query(y, baseURL)
    game_ID = content['fantasy_content']['game']['game_key']
    print game_ID
    return game_ID