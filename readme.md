## Yahoo Fantasy Football Wrapper
#### Current Status
Not in primetime shape... Been a side project over the past few months - whenever I get an hour or two, I dive in.  Hope to have time in future to fully polish and present.

#### Purpose
This collection of scripts is for my personal analysis of my 14 year fantasy football league.  Using these scripts, I am able to download the data from Yahoo efficiently, store it and use it for analysis and ad-hoc visualizations!

#### Scripts
##### _Master_Auth_
Script does everything related to creating a connection and making calls the the Yahoo API.

##### _Convenience_
Script contains many functions that are called from multiple scripts, particularly regarding database functions and url query formations.

##### _Master_Query_
This is the primary script that leverages _Master_Auth_ to make query requests, specifically for: Leagues, Teams, Scoreboard, Draft and Settings.  Requires the following:
- From Yahoo: consumer_key, consumer_secret, application_id
- Per your league: need to know the "gameid" and "leagueid" for each season in question.

During this script, all data is pickled.

##### _Master_Parse_
This script munges the data for Draft, Settings, and Scoreboard.  Particularly, the Scoreboard data is transformed from 1-line per matchup, to 2-lines per matchup (one line per team).  This enables much of the necessary aggregation needs of tools like Tableau.

##### _Settings_
Script contains some of the configuration items.  Here you need to list:
 - Team names to common names.
 - Database credentials.