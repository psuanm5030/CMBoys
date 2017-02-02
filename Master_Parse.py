# Purpose: (Not to be run independently) Provide the functions needed to prepare queries.
# Includes:
# 1. Scoreboard
# 2. Draft
# 3. Settings

import csv
import pandas as pd
from sqlalchemy import create_engine
import os
from collections import OrderedDict
import pprint as pp
import cPickle as pickle
from datetime import datetime
import Convenience

def sb_setup_dictionary(week, main):
    """
    Function to fill the dictionary of matchup details
    :param week: a weeks worth of matchup data
    :return: return dictionary
    """
    matchups = week['scoreboard']['matchups']['matchup']
    matchupsCnt = len(week['scoreboard']['matchups']['matchup'])
    cnt = 0

    for matchup in matchups:

        # Matchup week details:
        week = matchup['week']
        week_start = matchup['week_start']
        week_end = matchup['week_end']
        is_playoffs = matchup['is_playoffs']
        is_consolation = matchup['is_consolation']
        is_tied = matchup['is_tied']
        winner = matchup['winner_team_key']
        temp = matchup['winner_team_key'][-2:]
        winner_id = temp.replace('.', '')  # to account for winners with two digits
        id = week_end + ' - ' + str(cnt + 1)
        year = id[:4]
        # m_keys.append(id) # create a listing of the keys

        # Setup Trigger
        trigger = True
        for team in (matchup['teams']['team']):
            # print team # Ordered dictionary
            # print team['name']
            # print team['team_id']
            # print team['team_points']['total']
            # print team['team_points']['week']
            # print team['team_projected_points']['total']
            # print 'TEAM --->',team
            # Fill Team1 and Team2 Values:
            # Use try / except if there are no projected points for the period.

            try:  # No projected points in 2003 (no schema and no data for 2004 and 2005) - so replacing with 0
                if trigger:
                    team1 = [team['name'],
                             team['team_id'],
                             team['team_points']['total'],
                             team['team_projected_points']['total']]
                else:
                    team2 = [team['name'],
                             team['team_id'],
                             team['team_points']['total'],
                             team['team_projected_points']['total']]
            except:
                if trigger:
                    team1 = [team['name'],
                             team['team_id'],
                             team['team_points']['total'],
                             0.00]
                else:
                    team2 = [team['name'],
                             team['team_id'],
                             team['team_points']['total'],
                             0.00]

            # Reset Trigger
            trigger = False

        # Add to dictionary
        main[id] = {'week_details': {'week': week,
                                     'year': year,
                                     'week_start': week_start,
                                     'week_end': week_end,
                                     'is_playoffs': is_playoffs,
                                     'is_consolation': is_consolation,
                                     'is_tied': is_tied,
                                     'winner': winner,
                                     'winner_id': winner_id,
                                     'actual_var':abs(float(team1[2]) - float(team2[2])),
                                     'projected_var':abs(float(team1[3]) - float(team2[3]))},
                    'team1_details': {'name': team1[0],
                                      'team_id': team1[1],
                                      'team_actuals_points': team1[2],
                                      'team_projected_points': team1[3]},
                    'team2_details': {'name': team2[0],
                                      'team_id': team2[1],
                                      'team_actuals_points': team2[2],
                                      'team_projected_points': team2[3]}}

        cnt += 1
    return (main)

def sb_dict_to_df(dict1):
    """
    Turn the Dict of details into a dataframe.
    :param dict1: Dict including 3 levels of data
    :return: dataframe
    """
    newDict = {}
    for key in dict1.iterkeys():
        newDict[key] = {
            'is_consolation': dict1[key]['week_details']['is_consolation'],
            'is_playoffs': dict1[key]['week_details']['is_playoffs'],
            'is_tied': dict1[key]['week_details']['is_tied'],
            'year': dict1[key]['week_details']['year'],
            'week': dict1[key]['week_details']['week'],
            'week_end': dict1[key]['week_details']['week_end'],
            'week_start': dict1[key]['week_details']['week_start'],
            'winner': dict1[key]['week_details']['winner'],
            'winner_id': dict1[key]['week_details']['winner_id'],
            'team1_name': dict1[key]['team1_details']['name'],
            'team1_id': dict1[key]['team1_details']['team_id'],
            'team1_actual_points': dict1[key]['team1_details']['team_actuals_points'],
            'team1_projected_points': dict1[key]['team1_details']['team_projected_points'],
            'team2_name': dict1[key]['team2_details']['name'],
            'team2_id': dict1[key]['team2_details']['team_id'],
            'team2_actual_points': dict1[key]['team2_details']['team_actuals_points'],
            'team2_projected_points': dict1[key]['team2_details']['team_projected_points'],
            'actual_var': dict1[key]['week_details']['actual_var'],
            'projected_var': dict1[key]['week_details']['projected_var']
        }

    return pd.DataFrame(newDict).transpose()

def sb_write_two_line_csv(filein, fileout):
    """
    Function to read in the created csv and make each matchup two lines, instead of one.
    :param filein:
    :param fileout:
    :return:
    """
    with open(filein, 'rb') as fin:
        reader = fin.readlines()
        header = reader[0].rstrip().split(',')
        header[0] = 'matchup'
        cnt = 0
        with open(fileout, 'wb') as fout:
            writer = csv.writer(fout, lineterminator='\n')
            new_header = ['matchup', 'is_consolation', 'is_playoffs', 'is_tied', 'week', 'week_end', 'week_start',
                          'winner', 'winner_id', 'year', 'team_actual_points', 'team_id', 'team_name',
                          'team_projected_points', 'winner_team_name', 'loser_team_name', 'winner_flag',
                          'actual_var','projected_var']
            writer.writerow(new_header)
            reader.pop(0)  # Remove the original header
            # matchup,actual_var,is_consolation,is_playoffs,is_tied,projected_var,team1_actual_points,team1_id,team1_name,team1_projected_points,team2_actual_points,team2_id,team2_name,team2_projected_points,week,week_end,week_start,winner,winner_id,year
            for row in reader:
                row_cln = row.rstrip()
                row_list = row_cln.split(',')
                # print 'row - ',row_list

                # Figure out the winners / losers name
                if row_list[18] == row_list[11]:
                    winner_name = row_list[12]
                    loser_name = row_list[8]
                else:
                    winner_name = row_list[8]
                    loser_name = row_list[12]

                winner_flag = 0
                # Separate into two rows (18 items in list)
                row1 = [row_list[0], row_list[2], row_list[3], row_list[4], row_list[14], row_list[15],
                        row_list[16],
                        row_list[17], row_list[18], row_list[19], row_list[6], row_list[7], row_list[8],
                        row_list[9],
                        winner_name, loser_name, winner_flag,row_list[1], row_list[5]]
                row2 = [row_list[0], row_list[2], row_list[3], row_list[4], row_list[14], row_list[15],
                        row_list[16],
                        row_list[17], row_list[18], row_list[19], row_list[10], row_list[11], row_list[12], row_list[13],
                        winner_name, loser_name, winner_flag,row_list[1], row_list[5]]

                # Update the winner_flag!
                if row1[12] == row1[14]:
                    row1[16] = 1
                else:
                    row2[16] = 1

                writer.writerow(row1)
                writer.writerow(row2)
                cnt += 1
    return

def sb_main_parse(data):
    """
    Cycle through all the matchups (week-by-week) to parse / clean the data.
    :return:
    """
    global year_matchups # Make this a global variable to ensure it can be appended throughout
    matchup_keys = []
    year_matchups = {}
    for week in range(len(data)):
        # Check if the end-date of the matchup is past today's date and skip if so.
        try:
            debug = data[week][u'scoreboard'][u'matchups'][u'matchup']
        except:
            continue

        t1 = debug[0]['week_end']
        end_date = datetime.strptime(t1, "%Y-%m-%d").date()
        if end_date < present:
            year_matchups = sb_setup_dictionary(data[week], year_matchups)
    return year_matchups

def team_check_names():
    """
    Purpose of this script is to check and ensure all names are matched.  If not, user is prompted to complete.
    :return:
    """
    # Open the most recent mapped data pickle
    with open("/Users/Miller/GitHub/CMboys/data/pkl_mapping_names","rb") as f:
        manual_list = pickle.load(f)

    # Open the teams data
    with open("/Users/Miller/GitHub/CMboys/data/pkl_raw_teams","rb") as f:
        team_raw = pickle.load(f)

    established = manual_list.keys()
    proper_names = set(manual_list.values())
    yahoo_names = set([i['name'] for i in team_raw])

    for y_name in yahoo_names:
        if y_name in established:
            # print 'passing on: ' + y_name
            pass
        else:
            print 'Who is this --> ' + y_name
            print 'Type one of these: '
            print proper_names
            determination = raw_input('Proper Name: ').title()

            # Write new values to manual file
            manual_list[y_name] = determination
            print 'pkl_mapping_names updated for ' + determination + ' with the name: ' + y_name

    # Update the pickled file with the additions
    with open("/Users/Miller/GitHub/CMboys/data/pkl_mapping_names","wb") as f:
        pickle.dump(manual_list, f)

    print 'Manual Team Linkage File updated or already correct.'
    return

def sb_add_common_names(filein):
    """
    Adds common names to the two line matchup data.
    :return: DataFrame with common names
    """

    # Combine Teams data with Common Name (Manual) name data
    teams = pd.read_csv('/Users/Miller/GitHub/CMboys/data/teams.csv')
    teams['url'] = teams['url'].str.replace('http:', 'https:')  # replace http with https
    link = pd.read_csv('/Users/Miller/GitHub/CMboys/data/Manual Team Linkage.csv')
    common_teams = pd.merge(teams, link, how='left', left_on='name', right_on='Team1 Name')
    common_teams.to_csv('/Users/Miller/GitHub/CMboys/data/teams_with_common.csv')

    # This section merges the scoreboard two line data with the common team names
    standings = pd.read_csv('/Users/Miller/GitHub/CMboys/data/Standings_Manual.csv')
    data = pd.read_csv(filein)

    # Join in the Primary Team Common Name
    newSB = pd.merge(data, common_teams,
                     how='left',
                     left_on=['team_name', 'year'],
                     right_on=['name', 'season'])
    del newSB['Team1 Name']
    newSB.rename(columns={"Common Name": "Common Team Primary"}, inplace=True)
    # print newSB.info()

    # Join in the Winner Team Common Name
    newSB2 = pd.merge(newSB, link,
                      how='left',
                      left_on='winner_team_name',
                      right_on='Team1 Name')
    del newSB2['Team1 Name']
    newSB2.rename(columns={"Common Name": "Common Team Winner"}, inplace=True)
    # print newSB2.info()

    # Join in the Loser Team Common Name
    newSB3 = pd.merge(newSB2, link,
                      how='left',
                      left_on='loser_team_name',
                      right_on='Team1 Name')
    del newSB3['Team1 Name']
    newSB3.rename(columns={"Common Name": "Common Team Loser"}, inplace=True)
    # print newSB3.info()

    # Join in the Place Achieved (both "Finish" and "Place_Digits")
    newSB4 = pd.merge(newSB3, standings,
                      how='left',
                      left_on=['Common Team Primary', 'season'],
                      right_on=['Common Name', 'Season'])
    del newSB4['Season']
    del newSB4['Common Name']
    newSB4.rename(columns={"Common Name": "Common Team Loser"}, inplace=True)
    # print newSB4.info()

    # Setup a Primary Key
    newSB4['Primary_Key'] = newSB4['matchup'] + '-' + newSB4['team_key']
    newSB5 = newSB4.set_index('Primary_Key')

    return newSB5

def draft_main_parse(filein,csv_filepath):
    """
    Purpose of this is to arrange the data neatly from the draft results query pull.
    :param filein: File Location of the original draft results query data from "Master_Query.py"
    :return: outputs a CSV file to the drive
    """
    # todo this code is poorly done.  The issue is that there are 200+ rows in df when there should be just one per season. I account for this below by keeping only certain records
    df = pd.read_csv(filein)
    header = ['season','url','pick','round','team_key','player_key']

    final_list = []
    # Cycle through each row (and each pick within):
    for i in range(len(df)):
        final_line = []
        main = []
        raw = df.iloc[i:i + 1]
        line = eval(raw['draft_results'][i])

        # Cycle through each pick
        for pick in line['draft_result']:
            main = []
            # Add MAIN items from above
            main = [raw.iloc[0]['season'], raw.iloc[0]['url']]

            # Add New Items from within
            main.extend(pick.values())

            final_line.append(main)

        # Append data before starting on new season
        final_list.append(final_line)

    # De-duplication - take only first list for each season
    new_final_list = []
    current_season = final_list[0][0][0]
    new_final_list.append(final_list[0])
    for item in final_list:
        if item[0][0] == current_season:
            pass
        else:
            new_final_list.append(item)
            current_season = item[0][0]

    # Output to CSV with headers
    with open(csv_filepath, 'wb') as f:
        wr = csv.writer(f)
        wr.writerow(header)
        for list in new_final_list:
            wr.writerows(list)



# MASTER RUN FUNCTIONS

def team_MASTER_Parse_Database(raw):
    """
    Purpose is for an independent function related to setting up the teams file ("Team_Results_Parsed").
      Functions:
        - Parse the Teams File from the pickled query data.
        - Check if there are any teams without common name
        -
    :param raw: filepath of pickled data
    :return:
    """
    team_check_names() # Run the check names function
    with open("/Users/Miller/GitHub/CMboys/data/pkl_mapping_names","rb") as f:
        common_names = pickle.load(f)

    with open("/Users/Miller/GitHub/CMboys/data/pkl_raw_teams","rb") as f:
        teams_raw = pickle.load(f)

    # Cycle through the teams and add "common name":
    for i in range(len(teams_raw)):
        if 'Common Name' not in teams_raw[i]: # add the key if it doesnt exist
            teams_raw[i]['Common Name'] = common_names[teams_raw[i]['name']]
    # Update the pickle
    with open("/Users/Miller/GitHub/CMboys/data/pkl_raw_teams","wb") as f:
        pickle.dump(teams_raw,f)

    # DATABASE: TEAMS WITH COMMON
    df = pd.DataFrame(teams_raw)
    # df = pd.read_csv('/Users/Miller/GitHub/CMboys/data/teams_with_common.csv',
    #                  index_col=0)  # Import CSV data to Dataframe
    df['Primary_Key'] = df['team_key'] + ' - ' + df['Common Name']
    df.index = df['Primary_Key']
    engine = Convenience.db_connect()
    Convenience.db_setup_new('teams', df, engine)
    # Convenience.db_add_rows('teams', df, engine) # Add rows

def sb_MASTER_Parse_Database(raw):
    """
    This is the MASTER function that parses the Scoreboard data.
    Primary functions:
        - load the raw pickle data
        - clean-up each matchup
        - create 1-line matchup CSV
        - create 2-line matchup CSV
        - create teams_with_common file (user interaction if name is not identified
        - add common names to matchup data
        - send both (1-line and 2-line) to database
        - send teams_with_common CSV to database
    :param raw: filepath of pickled data
    :return: nothing (CSVs are created | database is populated)
    """

    # Load the pickled scoreboard data
    with open(raw, 'rb') as f:
        matchups_data = pickle.load(f)

    # Check date to exclude items that arent finished (end_date for matchup is past present).
    global present
    present = datetime.now().date()

    # Cycle through all the matchups to clean data
    raw_scoreboard_1 = sb_main_parse(matchups_data) # Cycle through and clean matchups data
    raw_scoreboard_2 = sb_dict_to_df(raw_scoreboard_1) # Turn from dictionary to Dataframe
    raw_scoreboard_2.to_csv('/Users/Miller/GitHub/CMboys/data/Scoreboard_OneLine.csv')

    # Add common names to the scoreboard data.
    sb_write_two_line_csv('/Users/Miller/GitHub/CMboys/data/Scoreboard_OneLine.csv',
                       '/Users/Miller/GitHub/CMboys/data/Scoreboard_TwoLines.csv')
    team_check_names() # Runs script to prompt user for names.
    # Add the common names to the file
    scoreboard_final = sb_add_common_names('/Users/Miller/GitHub/CMboys/data/Scoreboard_TwoLines.csv')
    scoreboard_final.to_csv('/Users/Miller/GitHub/CMboys/data/Scoreboard_TwoLines_FINAL.csv')

    # DATABASE: SCOREBOARD
    df = pd.read_csv('/Users/Miller/GitHub/CMboys/data/Scoreboard_TwoLines_FINAL.csv')  # Import CSV data to Dataframe
    engine = Convenience.db_connect()
    Convenience.db_setup_new('scoreboard', df, engine)
    # todo add 1-line scoreboard data to database (make sure to get common names in there)
    # Convenience.db_add_rows('scoreboard', df, engine) # Add rows

    # DATABASE: TEAMS WITH COMMON
    df = pd.read_csv('/Users/Miller/GitHub/CMboys/data/teams_with_common.csv',index_col=0)  # Import CSV data to Dataframe
    df['Primary_Key'] = df['team_key'] + ' - ' + df['Common Name']
    engine = Convenience.db_connect()
    Convenience.db_setup_new('teams', df, engine)
    # Convenience.db_add_rows('teams', df, engine) # Add rows

def draft_MASTER_Parse_Database(raw):
    """
    This is the MASTER function that parses the Draft data.
    Primary functions:
        - load the raw csv data to be parsed
        - organize the draft query data
        - output to CSV
        - send CSV data
    :param raw: filepath of CSV data
    :return: nothing (CSVs are created | database is populated)
    """
    # Handle Draft Results
    draft_main_parse(raw,
                     '/Users/Miller/GitHub/CMboys/data/Draft_Results_Parsed.csv')
    df = pd.read_csv('/Users/Miller/GitHub/CMboys/data/Draft_Results_Parsed.csv')  # Import CSV data to Dataframe
    df.index.name = 'index'
    df['season'] = df['season'].astype(str)
    df['pick'] = df['pick'].astype(str)
    df['Primary_Key'] = df['season'] + ' - ' + df['pick']
    engine = Convenience.db_connect()
    Convenience.db_setup_new('draft', df, engine)
    # Convenience.db_add_rows('teams', df, engine) # Add rows

def settings_MASTER_Parse_Database(raw):
    """
    Simply sends the settings data - previously queried into the database
    :param raw: filepath of CSV data
    :return: nothing (database is populated)
    """
    df = pd.read_csv(raw)
    engine = Convenience.db_connect()
    Convenience.db_setup_new('settings', df, engine)
    # Convenience.db_add_rows('teams', df, engine) # Add rows


# todo draft_main_parse() takes too long

