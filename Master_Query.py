import csv
import Master_Auth
import os
import datetime
import cPickle as pickle
import pandas as pd
import Convenience
import Master_Parse
import settings
import unicodecsv

csv.register_dialect('ALM', delimiter=',', quoting=csv.QUOTE_ALL)

y = Master_Auth.yahoo_session()

def getLeagueData(seasons):
    """

    :return:
    """
    leagues = []

    for season, details in seasons.iteritems():
        # LEAGUE DATA
        league_code = Convenience.URL_make_league_code(details['gameid'], details['leagueid'])
        l = Master_Auth.api_query(y, Convenience.URL_league_data(league_code))  # Actual Yahoo Query
        this_league = l['fantasy_content']['league']  # grab relevant part of dict

        leagues.append(this_league)
        last_day = datetime.datetime.strptime(this_league['end_date'], "%Y-%m-%d")
        # go one beyond last day to make sure you get all the roster moves.
        last_day = last_day + datetime.timedelta(days=1)

        print 'Finished with ' + str(season) + ' season of LEAGUE DATA.'

    # Pickle the raw file
    createPickle('leagues', leagues)

    return

def getDraftData(seasons):
    """
    Query for draft results.  One for each year.
    :return:
    """
    draft = []
    for season, details in seasons.iteritems():
        # Get League_Code
        league_code = Convenience.URL_make_league_code(details['gameid'], details['leagueid'])

        s = Master_Auth.api_query(y, Convenience.URL_draft_results(league_code))
        # grab relevant part of dict
        this_draft = s['fantasy_content']['league']
        draft.append(this_draft)

        print 'Finished with ' + str(season) + ' season of DRAFT RESULTS.'

    # Pickle the raw file
    createPickle('draft', draft)

    return

def getScoreboardData(seasons):
    """
    Query for score results.  One per team per week, per year.
    :return: list of data for scoreboard
        Structure: list of dictionaries - one per week (16 weeks per season - if complete).  Each dictionary contains up to six matchups (each a dictionary)
    """
    scoreboard = []
    for season, details in seasons.iteritems():
        league_code = Convenience.URL_make_league_code(details['gameid'], details['leagueid'])

        # Scoreboard data
        for wk in range(1, last_week + 1):
            s = Master_Auth.api_query(y, Convenience.URL_league_scoreboard(league_code, wk))
            # grab relevant part of dict
            this_scoreboard = s['fantasy_content']['league']
            scoreboard.append(this_scoreboard)

        print 'Finished with ' + str(season) + ' season of SCOREBOARD RESULTS.'

    # Pickle the raw file
    createPickle('scoreboard', scoreboard)

    return

def getTeamsData(seasons):
    """
    Query for the Teams data in each year.
    :return:
    """
    # Setup common team Mapping
    if os.path.isfile("/Users/Miller/GitHub/CMboys/data/pkl_mapping_names"):
        with open("/Users/Miller/GitHub/CMboys/data/pkl_mapping_names", 'rb') as f:
            common_name_mapping = pickle.load(f)
    else:
        with open("/Users/Miller/GitHub/CMboys/data/pkl_mapping_names", 'wb') as f:
            pickle.dump(settings.COMMON_NAMES,f)


    # todo Could clean-up this query.  Not sure about all the various steps that are being done here
    teams = []

    for season, details in seasons.iteritems():
        league_code = Convenience.URL_make_league_code(details['gameid'], details['leagueid'])
        l = Master_Auth.api_query(y, Convenience.URL_league_data(league_code)) #Actual Yahoo Query
        this_league = l['fantasy_content']['league'] #grab relevant part of dict

        # Teams Data
        num_teams = int(this_league['num_teams'])
        for j in range(1, num_teams + 1):
            # get basic team data
            team_code = Convenience.URL_make_team_code(details['gameid'], details['leagueid'], j)
            t = Master_Auth.api_query(y, Convenience.URL_team_data(team_code))
            # just relevant response
            this_team = t['fantasy_content']['team']
            # include season in dict
            this_team['season'] = this_league['season']
            this_team['logo'] = this_team['team_logos']['team_logo']['url']

            # handle co-managers
            this_manager = this_team['managers']['manager']
            if type(this_manager) == list:
                this_manager = this_manager[0]

            this_team['manager_id'] = this_manager['manager_id']

            this_team['manager_nickname'] = this_manager['nickname']
            if 'guid' in this_manager: manager_guid = this_manager['guid']
            if 'guid' not in this_manager: manager_guid = None
            this_team['manager_guid'] = manager_guid
            if 'email' in this_manager: manager_email = this_manager['email']
            if 'email' not in this_manager: manager_email = None
            this_team['manager_email'] = manager_email
            if "is_owned_by_current_login" not in this_team: this_team["is_owned_by_current_login"] = None
            # drop some keys
            this_team.pop("managers", None)
            this_team.pop("team_logos", None)
            this_team.pop("roster_adds", None)

            # print str(this_manager['nickname']) + " - " + this_team['season']
            teams.append(this_team)

        print 'Finished with ' + str(season) + ' season of TEAMS RESULTS.'

    # Pickle the raw file
    createPickle('teams', teams)

    return

def getSettingsData(seasons):
    """
    Runs through all seasons and pulls settings through a number of API requests
    :return: dictionary of dataframes
        Structured like:
            {'2016': dataframe,
            '2015': dataframe}
    """
    settingsResults = []
    settingsResultsComprehensive = {}
    header = ['season','stat_id','enabled','name','display_name','sort_order','position_type','stat_position_type','value'] #value is in "stat_modifiers


    for season, details in seasons.iteritems():
        settingsResults = []
        league_code = Convenience.URL_make_league_code(details['gameid'], details['leagueid'])

        s = Master_Auth.api_query(y, Convenience.URL_settings(league_code))

        this_set1 = s['fantasy_content']['league']['settings']['stat_categories']['stats']['stat']
        this_set2 = s['fantasy_content']['league']['settings']['stat_modifiers']['stats']['stat']

        df1 = pd.DataFrame(this_set1)
        df2 = pd.DataFrame(this_set2)
        df = pd.merge(df1, df2, how='left', on='stat_id')

        try:
            df['stat_position_types'] = df['stat_position_types'].map(
            lambda v: v['stat_position_type']['position_type'])
        except:
            pass


        print 'Finished with ' + season + ' of SETTINGS RESULTS.'
        settingsResultsComprehensive[season] = df

    # Setup Master Dataframe
    df = pd.concat(settingsResultsComprehensive)
    df.reset_index(inplace=True)  # reset the multiindex
    del df['level_1']  # remove the previous index that was level_1
    df.columns.values[0] = 'season'  # rename the first column
    df['Primary_Key'] = df['season'] + ' - ' + df['stat_id']

    # Create a list of lists with header
    header = ['season','display_name','enabled','is_excluded_from_display','is_only_display_stat','name','position_type','sort_order','stat_id','stat_position_types','value','Primary_Key']
    settings = df.values.tolist() # Convert back to a list of lists and append
    settings.insert(0,header)

    # Pickle the raw file
    createPickle('settings', settings)

    return

def createPickle(name, list_data):
    """
    Simply sends the data to a pickle file
    :param name:
    :return:
    """
    # Add item to the CSV Selections list (anytime something is pickled, it is available for CSV writing)
    csv_selections.append(name)
    # Pickle the scoreboard dictionary for later usage
    pkl_name = '/Users/Miller/GitHub/CMboys/data/pkl_raw_' + name
    with open(pkl_name, 'wb') as f:
        pickle.dump(list_data, f)

    return

# Delete this when done.
def getData(min_season):

    # Testing Connection
    # try:
    #     print 'try'
    #     league_code = make_league_code(i['gameid'], i['leagueid'])
    #     l = auth.api_query(y, league_data(league_code)) #Actual Yahoo Query
    #     this_league = l['fantasy_content']['league']  # grab relevant part of dict
    # except:
    #     print 'Authentication has expired.  Deleting auth.yml and starting again.'
    #     os.remove('/Users/Miller/GitHub/CMboys/auth.yml')
    #     y = auth.yahoo_session()
    #     # raise NameError('Authentication has expired.  Delete auth.yml and start again.')

    for i in new_spec:
        # LEAGUE DATA
        league_code = Convenience.URL_make_league_code(i['gameid'], i['leagueid'])
        l = Master_Auth.api_query(y, Convenience.URL_league_data(league_code)) #Actual Yahoo Query
        this_league = l['fantasy_content']['league'] #grab relevant part of dict

        leagues.append(this_league)
        last_day = datetime.datetime.strptime(this_league['end_date'], "%Y-%m-%d")
        #go one beyond last day to make sure you get all the roster moves.
        last_day = last_day + datetime.timedelta(days=1)


        # Players
        # for itm in range(1,last_week + 1):
        #     s = auth.api_query(y, players(league_code))
        #     #grab relevant part of dict
        #     this_playerset = s['fantasy_content']['league']
        #     playerResults.append(this_playerset)

        # # Transactions - Commented out for now
        # for itm in range(1,last_week + 1):
        #     s = auth.api_query(y, transactions(league_code))
        #     #grab relevant part of dict
        #     this_transset = s['fantasy_content']['league']
        #     transResults.append(this_transset)



            # # Roster - Commented out for now
            # # Erroring out on list (wants dict)... You should debug and check out the write to csv function in Auth.
            #
            # for itm in range(1,last_week + 1):
            #     r = auth.api_query(y, week_roster(team_code,itm))
            #     #grab relevant part of dict
            #     this_roster = r['fantasy_content']['team']['roster']['players']['player']
            #     for k in this_roster:
            #         k['owner_email'] = manager_email
            #         k['owner_guid'] = manager_guid
            #         k['team_code'] = team_code
            #         k['date_captured'] = last_day
            #         k['season'] = this_league['season']
            #         k['full_name'] = k['name']['full']
            #         k['first_name'] = k['name']['ascii_first']
            #         k['last_name'] = k['name']['ascii_last']
            #         k['image_url'] = k['headshot']['url']
            #         k['eligible_positions'] = k['eligible_positions']['position']
            #         k['selected_position'] = k['selected_position']['position']
            #         if "status" not in k: k["status"] = None
            #         if "starting_status" not in k: k["starting_status"] = None
            #         if "has_player_notes" not in k: k["has_player_notes"] = None
            #         if "has_recent_player_notes" not in k: k["has_recent_player_notes"] = None
            #         if "on_disabled_list" not in k: k["on_disabled_list"] = None
            #         if "is_editable" not in k: k["is_editable"] = None
            #         k.pop("headshot", None)
            #         k.pop("name", None)
            #         k.pop("editorial_player_key", None)
            #         k.pop("editorial_team_key", None)
            #         rosterResults.append(this_roster)

            # ANDY - commenting out as this is more baseball related!!
            # When ready - you can copy the scoreboard section to iterate by Week!
            #get team roster
            # r = auth.api_query(y, roster_data(team_code, last_day))
            # this_roster = r['fantasy_content']['team']['roster']['players']['player']
            # for k in this_roster:
            #     k['owner_email'] = manager_email
            #     k['owner_guid'] = manager_guid
            #     k['team_code'] = team_code
            #     k['date_captured'] = last_day
            #     k['season'] = this_league['season']
            #     k['full_name'] = k['name']['full']
            #     k['first_name'] = k['name']['ascii_first']
            #     k['last_name'] = k['name']['ascii_last']
            #     k['image_url'] = k['headshot']['url']
            #     k['eligible_positions'] = k['eligible_positions']['position']
            #     k['selected_position'] = k['selected_position']['position']
            #     if "status" not in k: k["status"] = None
            #     if "starting_status" not in k: k["starting_status"] = None
            #     if "has_player_notes" not in k: k["has_player_notes"] = None
            #     if "has_recent_player_notes" not in k: k["has_recent_player_notes"] = None
            #     if "on_disabled_list" not in k: k["on_disabled_list"] = None
            #     if "is_editable" not in k: k["is_editable"] = None
            #     k.pop("headshot", None)
            #     k.pop("name", None)
            #     k.pop("editorial_player_key", None)
            #     k.pop("editorial_team_key", None)
            #     rosters.append(k)
    return

def data_to_csv(target_dir, data_to_write, desired_name):
    """
    Convenience function to write a dict to CSV with appropriate parameters.
    :param target_dir: directory to write to
    :param data_to_write: data passed
    :param desired_name: name of CSV file
    :return: nothing (CSV is written to file at desired directory)
    """
    global d
    if len(data_to_write) == 0:
        print 'No data.  Therefore nothing was written to CSV for: ' + desired_name + ' (target dir: '+ target_dir + ').'
        return None
    if not os.path.exists(target_dir): # Create directory if doesnt exist
        os.makedirs(target_dir)

    # Data is Dictionary (NOT SURE THIS IS WORKING)
    if type(data_to_write) == dict:
        #order dict by keys
        d = OrderedDict(sorted(data_to_write.items()))
        keys = set().union(*(i.keys() for i in d))

    # Data is List of Lists (used for "settings")
    if any(isinstance(el, list) for el in data_to_write):
        with open("%s/%s.csv" % (target_dir, desired_name), 'wb') as f:
            writer = csv.writer(f)
            writer.writerows(data_to_write)
        return
    elif type(data_to_write) == list: # if not list of lists, then check if just list
        d = data_to_write
        keys = data_to_write[0].keys()
    else:
        print 'Cannot handle data - check type of object pushed here.'

    # Write to CSV
    with open("%s/%s.csv" % (target_dir, desired_name), 'wb') as f:
        dw = unicodecsv.DictWriter(f, keys, dialect='ALM',restval='', extrasaction='ignore') #paramters for missing vals: restval='' and extrasaction='ignore'
        dw.writeheader()
        if type(data_to_write) == dict:
            dw.writerow(d)
        if type(data_to_write) == list:
            dw.writerows(d)
    f.close()


# Primary Run Code
if __name__ == '__main__':

    # Base Parameters (some commented out as not used yet)
    # rosters = []
    # rosterResults = []
    # playerResults = []
    # transResults = []
    last_week = 16
    global csv_selections
    csv_selections = []

    # Build out the seasons list
    # todo GUI here for selecting the seasons - right now set to range
    base_seasons = {'2016': {'gameid': 359, 'leagueid': 67045},
               '2015': {'gameid': 348, 'leagueid': 25151},
               '2014': {'gameid': 331, 'leagueid': 10856},
               '2013': {'gameid': 314, 'leagueid': 4794},
               '2012': {'gameid': 273, 'leagueid': 30250},
               '2011': {'gameid': 257, 'leagueid': 24929},
               '2010': {'gameid': 242, 'leagueid': 75801},
               '2009': {'gameid': 222, 'leagueid': 282501},
               '2008': {'gameid': 199, 'leagueid': 31052},
               '2007': {'gameid': 175, 'leagueid': 68086},
               '2006': {'gameid': 153, 'leagueid': 378},
               '2005': {'gameid': 124, 'leagueid': 59995},
               '2004': {'gameid': 101, 'leagueid': 94381},
               '2003': {'gameid': 79, 'leagueid': 31137}}
    # todo add the base_seasons data to the settings file or credentials file
    start = 2003
    stop = 2016
    run = map(str, [x for x in range(start,stop+1)]) # create a string listing of seasons
    # Create the specs listing
    seasons = {}
    for r in run:
        if r in base_seasons.keys():
            seasons[r] = base_seasons[r]

    # Run Yahoo Queries
    # getLeagueData(seasons)
    getTeamsData(seasons)
    # getScoreboardData(seasons)
    # getDraftData(seasons)
    # getSettingsData(seasons) # todo need to send data to database

    # Checklist:
    # getLeagueData() - DONE
    # getDraftData() - DONE
    # getTeamsData() - DONE
    # getScoreboardData() - DONE
    # getSettingsData() - DONE
    # todo getTransactionsData()
    # todo getRosterData()


    # Write data to CSV
    # csv_selections list is generated when the createPickle() function is run
    directory = 'data'
    for item in csv_selections:
        with open('/Users/Miller/GitHub/CMboys/data/pkl_raw_%s' % (item), 'rb') as f: # Load the pickle
            data = pickle.load(f)
            data_to_csv(directory,data,item) # send to CSV

    # Run Parsing / Database Imports for applicable
    # Teams are imported to database along with Scoreboard
    if 'team' in csv_selections:
        Master_Parse.team_MASTER_Parse_Database('/Users/Miller/GitHub/CMboys/data/pkl_raw_teams')
    if 'scoreboard' in csv_selections:
        Master_Parse.sb_MASTER_Parse_Database('/Users/Miller/GitHub/CMboys/data/pkl_raw_scoreboard')
    if 'draft' in csv_selections:
        Master_Parse.draft_MASTER_Parse_Database('/Users/Miller/GitHub/CMboys/data/draft.csv')
    if 'settings' in csv_selections:
        Master_Parse.settings_MASTER_Parse_Database('/Users/Miller/GitHub/CMboys/data/settings.csv')
    # todo players




    print '\nDONE WITH query.py'


    # Main List
    # todo bring in query: players, settings
    # todo some of the pulls dont need league details captured with (e.g., draft results)
    # todo Draft Results - new query (Right now it takes 12 results for each year, when only one is needed)
    # todo gui to choose which queries to run
    # todo Draft results - has player key as the main identifier - may need to add that in teams common?
    # todo Reformulate all.  Everything that is queried should be pickled, then CSV (named "XX_Raw.csv"), then should go through parsing, then should go into database.

    # Ideas
    # todo 1. Add in additional query information
    # todo 3. Make the league submissions better - incase we need to do this for other leagues
    # todo 4. Bringing in roster data
    # todo 5. Package this into an app?
    # todo 6. Remove unnecessary data
    # todo 7. Normalize points