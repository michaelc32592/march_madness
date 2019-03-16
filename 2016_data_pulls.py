# -*- coding: utf-8 -*-
"""
Created on Wed Dec 26 12:26:20 2018

@author: micha
"""

import pandas as pd
import psycopg2 as pg2
from sqlalchemy import create_engine

conn = pg2.connect(database = 'March_Madness' , user = 'postgres', password = 'crump83')
engine = create_engine('postgresql://postgres:crump83@localhost:5432/March_Madness')
cur = conn.cursor()

#figure out who won the most
#2016 analysis

#regular season success

    

sql170 = '''
SELECT "TeamID", "Seed" FROM "NCAATourneySeeds"
WHERE "Season" = 2016
'''


S2016_seed = pd.read_sql(sql170,conn)
    

#Average Team Stats (add rebounds, turnovers (both winner and loser), etc)
#WTO are offensive players

#winning team points scored Group By sort by desc
sql106 = '''
SELECT "WTeamID",AVG("WScore") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
ORDER BY  AVG("WScore") DESC
'''

S2016_points_scored_win = pd.read_sql(sql106, conn)
    

#winning team points allowed glroup by sort by desc
sql107 = '''
SELECT "WTeamID",AVG("LScore") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
ORDER BY  AVG("LScore") DESC
'''

S2016_points_scored_loss = pd.read_sql(sql107, conn)


#winning team rebounds
sql120 = '''
SELECT "WTeamID",AVG("WOR"+"WDR") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
ORDER BY  AVG("WOR"+"WDR") DESC
'''
S2016_w_rebounds = pd.read_sql(sql120,conn)

#do overall points (both winning and losing)
sql125 = '''
SELECT "WTeamID", SUM("WScore"),COUNT("WScore")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
'''
S2016_total_wpts = pd.read_sql(sql125,conn)
S2016_total_wpts['TeamID'] = S2016_total_wpts['WTeamID']
#losing sum/count

sql126 = '''
SELECT "LTeamID", SUM("LScore"),COUNT("LScore")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "LTeamID"
'''
S2016_total_lpts = pd.read_sql(sql126, conn)
S2016_total_lpts['TeamID'] = S2016_total_lpts['LTeamID']
S2016_total_avg_pts = pd.merge(S2016_total_wpts, S2016_total_lpts, how = 'inner', on = 'TeamID')
S2016_total_avg_pts['total_pts'] = S2016_total_avg_pts['sum_x'] + S2016_total_avg_pts['sum_y']
S2016_total_avg_pts['total_games'] = S2016_total_avg_pts['count_x'] + S2016_total_avg_pts['count_y']

S2016_total_avg_pts['Average Pts'] = S2016_total_avg_pts['total_pts']/S2016_total_avg_pts['total_games']
S2016_total_avg_pts = S2016_total_avg_pts[['TeamID','Average Pts']]

#Points allowed
sql127 = '''
SELECT "WTeamID", SUM("LScore"),COUNT("LScore")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
'''
S2016_points_allowed_w = pd.read_sql(sql127,conn)
S2016_points_allowed_w['TeamID'] = S2016_points_allowed_w['WTeamID']

sql128 = '''
SELECT "LTeamID", SUM("WScore"),COUNT("WScore")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "LTeamID"
'''
S2016_points_allowed_l = pd.read_sql(sql128,conn)
S2016_points_allowed_l['TeamID'] = S2016_points_allowed_l['LTeamID']
S2016_total_avg_pa = pd.merge(S2016_points_allowed_w,S2016_points_allowed_l, how = 'inner', on = 'TeamID')
#12/26-left off here
S2016_total_avg_pa['total_pts'] = S2016_total_avg_pa['sum_x'] + S2016_total_avg_pa['sum_y']
S2016_total_avg_pa['total_games'] = S2016_total_avg_pa['count_x'] + S2016_total_avg_pa['count_y']

S2016_total_avg_pa['Average PA'] = S2016_total_avg_pa['total_pts']/S2016_total_avg_pa['total_games']
S2016_total_avg_pa = S2016_total_avg_pa[['TeamID','Average PA']]

#join winning and losing sum columns and counts
#divide out


#ORB
sql129 = '''
SELECT "WTeamID", SUM("WOR"),COUNT("WOR")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
'''
S2016_o_rbs_w = pd.read_sql(sql129, conn)
S2016_o_rbs_w['TeamID'] = S2016_o_rbs_w['WTeamID']
sql130 = '''
SELECT "LTeamID", SUM("LOR"),COUNT("LOR")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2015
GROUP BY "LTeamID"
'''

S2016_o_rbs_l = pd.read_sql(sql130, conn)
S2016_o_rbs_l['TeamID'] = S2016_o_rbs_l['LTeamID']
S2016_o_rbs_for = pd.merge(S2016_o_rbs_w,S2016_o_rbs_l, how = 'inner', on = 'TeamID')
S2016_o_rbs_for['total_rbs'] = S2016_o_rbs_for['sum_x'] + S2016_o_rbs_for['sum_y']
S2016_o_rbs_for['total_games'] = S2016_o_rbs_for['count_x'] + S2016_o_rbs_for['count_y']
S2016_o_rbs_for['orpg'] = S2016_o_rbs_for['total_rbs']/S2016_o_rbs_for['total_games']
S2016_o_rbs_for = S2016_o_rbs_for[['TeamID','orpg']]

#offensive rebounds allowed
sql131 = '''
SELECT "WTeamID", SUM("LOR"),COUNT("LOR")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
'''
S2016_o_rbs_a_w = pd.read_sql(sql131, conn)
S2016_o_rbs_a_w['TeamID'] = S2016_o_rbs_a_w['WTeamID']


sql132 = '''
SELECT "LTeamID", SUM("WOR"),COUNT("WOR")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "LTeamID"
'''
S2016_o_rbs_a_l = pd.read_sql(sql132,conn)
S2016_o_rbs_a_l['TeamID'] = S2016_o_rbs_a_l['LTeamID']
S2016_o_rbs_against = pd.merge(S2016_o_rbs_a_w,S2016_o_rbs_a_l, how = 'inner', on = 'TeamID')
S2016_o_rbs_against['total_rb_a'] = S2016_o_rbs_against['sum_x'] + S2016_o_rbs_against['sum_y']
S2016_o_rbs_against['total_games'] = S2016_o_rbs_against['count_x'] + S2016_o_rbs_against['count_y']
S2016_o_rbs_against['orapg'] = S2016_o_rbs_against['total_rb_a']/S2016_o_rbs_against['total_games']
S2016_o_rbs_against= S2016_o_rbs_against[['TeamID','orapg']]

#Do check-look at points for/points against games 


#Turnovers
#turnovers made
sql133 = '''
SELECT "WTeamID", SUM("LTO"),COUNT("LTO")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
'''
S2016_to_w = pd.read_sql(sql133, conn)
S2016_to_w['TeamID'] =S2016_to_w['WTeamID']
sql134 = '''
SELECT "LTeamID", SUM("WTO"),COUNT("WTO")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "LTeamID"
'''

S2016_to_l = pd.read_sql(sql134, conn)
S2016_to_l['TeamID'] = S2016_to_l['LTeamID']
S2016_to_made = pd.merge(S2016_to_w,S2016_to_l, how = 'inner', on = 'TeamID')
S2016_to_made['total_tos_made'] = S2016_to_made['sum_x'] + S2016_to_made['sum_y']
S2016_to_made['total_games'] = S2016_to_made['count_x'] + S2016_to_made['count_y']
S2016_to_made['avg_to_made'] = S2016_to_made['total_tos_made']/S2016_to_made['total_games']
S2016_to_made = S2016_to_made[['TeamID','avg_to_made']]


#turnovers against
sql135 = '''
SELECT "WTeamID", SUM("WTO"),COUNT("WTO")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
'''
S2016_to_against_w = pd.read_sql(sql135, conn)
S2016_to_against_w['TeamID'] =S2016_to_against_w['WTeamID']

sql136 = '''
SELECT "LTeamID", SUM("LTO"),COUNT("LTO")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "LTeamID"
'''

S2016_to_against_l = pd.read_sql(sql136, conn)
S2016_to_against_l['TeamID'] = S2016_to_against_l['LTeamID']
S2016_to_against = pd.merge(S2016_to_against_w,S2016_to_against_l, how = 'inner', on = 'TeamID')
S2016_to_against['total_tos_against'] = S2016_to_against['sum_x'] + S2016_to_against['sum_y']
S2016_to_against['total_games'] = S2016_to_against['count_x'] + S2016_to_against['count_y']
S2016_to_against['avg_to_allowed'] = S2016_to_against['total_tos_against']/S2016_to_against['total_games']
S2016_to_against = S2016_to_against[['TeamID','avg_to_allowed']]



#winning team turnovers (turnovers made)
sql121 = '''
SELECT "WTeamID",AVG("LTO") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
ORDER BY  AVG("LTO") DESC
'''
S2016_WTO = pd.read_sql(sql121,conn)



#winning team home vs losing team home
sql108 = '''
SELECT "WTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016 AND "WLoc" = 'H'
GROUP BY "WTeamID"
ORDER BY COUNT("WLoc") DESC;
'''


sql109 = '''
SELECT "WTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016 AND "WLoc" = 'N'
GROUP BY "WTeamID"
ORDER BY COUNT("WLoc") DESC
;
'''
sql110 = '''
SELECT "WTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016 AND "WLoc" = 'A'
GROUP BY "WTeamID"
ORDER BY COUNT("WLoc") DESC
;
'''

sql160='''
SELECT "LTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016 AND "WLoc" = 'A'
GROUP BY "LTeamID"
ORDER BY COUNT("WLoc") DESC
;
'''
sql161 = '''
SELECT "LTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016 AND "WLoc" = 'H'
GROUP BY "LTeamID"
ORDER BY COUNT("WLoc") DESC
'''


S2016_home_wins = pd.read_sql(sql108, conn)
S2016_home_losses = pd.read_sql(sql160, conn)
S2016_neutral_wins = pd.read_sql(sql109, conn) 
S2016_neutral_wins.columns = ['WTeamID','Neutral']
S2016_away_wins = pd.read_sql(sql110, conn) 
S2016_away_losses = pd.read_sql(sql161, conn)



S2016_home_record = pd.merge(S2016_home_wins, S2016_home_losses, left_on = "WTeamID", right_on = "LTeamID")
S2016_home_record['Home Record'] = S2016_home_record['count_x']/(S2016_home_record['count_x'] + S2016_home_record['count_y'])
S2016_home_record['TeamID'] = S2016_home_record['WTeamID']
S2016_home_record = S2016_home_record[['TeamID','Home Record']]

S2016_away_record = pd.merge(S2016_away_wins, S2016_away_losses, left_on = "WTeamID", right_on = "LTeamID")
S2016_away_record['Away Record'] = S2016_away_record['count_x']/(S2016_away_record['count_x'] + S2016_away_record['count_y'])
S2016_away_record['TeamID'] = S2016_away_record['WTeamID']
S2016_away_record= S2016_away_record[['TeamID','Away Record']]
#do home record, away record
#build a function looking at location, only include the one that applies (ie away record if team is away)





#overall record
sql111 = '''
SELECT "WTeamID", COUNT("WTeamID") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "WTeamID"
ORDER BY COUNT("WTeamID") DESC
;
'''
wins = pd.read_sql(sql111, conn)
wins.columns = ['TeamID','Wins']

sql112 = '''
SELECT "LTeamID", COUNT("LTeamID") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2016
GROUP BY "LTeamID"
ORDER BY COUNT("LTeamID") DESC
;
'''
losses = pd.read_sql(sql112, conn)
losses.columns = ['TeamID','Losses']

record = pd.merge(wins,losses, on = "TeamID")
record["win_pct"] = record["Wins"]/(record["Wins"] + record["Losses"])
record.to_sql("2016 Record",engine) 





#strength of schedule (ie record of losing teams if they won and vice versa)

sql114 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2016 Record"."TeamID","2016 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2016 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2016 Record"."TeamID"
WHERE "Season" = 2016) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 

S2016_W_strength_of_schedule = pd.read_sql(sql114, conn)
S2016_W_strength_of_schedule['TeamID'] = S2016_W_strength_of_schedule['WTeamID']
S2016_W_strength_of_schedule['WSOS'] = S2016_W_strength_of_schedule['avg']
S2016_W_strength_of_schedule = S2016_W_strength_of_schedule[['TeamID','WSOS']]

sql115 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2016 Record"."TeamID","2016 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2016 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2016 Record"."TeamID"
WHERE "Season" = 2016) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
''' 

S2016_L_strength_of_schedule = pd.read_sql(sql115, conn)
S2016_L_strength_of_schedule['LSOS'] = S2016_L_strength_of_schedule['avg']
S2016_L_strength_of_schedule['TeamID'] = S2016_L_strength_of_schedule['LTeamID']
S2016_L_strength_of_schedule = S2016_L_strength_of_schedule[['TeamID','LSOS']]




#Coach Analysis
#Total Tourney Wins/Losses for Coaches
sql116 = r'''
SELECT "TeamCoaches"."CoachName",COUNT("NCAATourneyDetailedResults"."WTeamID")
FROM "TeamCoaches"
INNER JOIN "NCAATourneyDetailedResults"  ON "TeamCoaches"."TeamID" = "NCAATourneyDetailedResults"."WTeamID" AND "TeamCoaches"."Season" = "NCAATourneyDetailedResults"."Season"  
GROUP BY "CoachName"
ORDER BY "count" DESC
'''
S2016_coach_wins = pd.read_sql(sql116, conn)

sql117 = r'''
SELECT "TeamCoaches"."CoachName",COUNT("NCAATourneyDetailedResults"."LTeamID")
FROM "TeamCoaches"
INNER JOIN "NCAATourneyDetailedResults"  ON "TeamCoaches"."TeamID" = "NCAATourneyDetailedResults"."LTeamID" AND "TeamCoaches"."Season" = "NCAATourneyDetailedResults"."Season"  
GROUP BY "CoachName"
ORDER BY "count" DESC
'''
S2016_coach_losses = pd.read_sql(sql117, conn)

S2016_coach_record = pd.merge(S2016_coach_wins,S2016_coach_losses, on = "CoachName")
S2016_coach_record["coach_win_pct"] = S2016_coach_record["count_x"]/(S2016_coach_record["count_x"]+ S2016_coach_record["count_y"]) 
S2016_coach_record = S2016_coach_record[['CoachName','coach_win_pct']]
sql171 = r'''
SELECT * FROM "TeamCoaches" 
WHERE "Season" = 2017

'''
S2016_team_coach = pd.read_sql(sql171, conn)
S2016_team_coach_record = pd.merge(S2016_coach_record, S2016_team_coach, on = "CoachName")
S2016_team_coach_record = S2016_team_coach_record[['TeamID','coach_win_pct']]


#play by play
#efficiency
#reliability on one player

#winning clutch made fts
#MAKE SURE TO CHANGE TO 16 ONCE DOWNLOADED
sql122 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents16"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

S2016_clutch_ft_made = pd.read_sql(sql122, conn)

sql123 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents16"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
S2016_clutch_ft_miss = pd.read_sql(sql123, conn)

S2016_clutch_ft = pd.merge(S2016_clutch_ft_made,S2016_clutch_ft_miss, on = "WTeamID")
S2016_clutch_ft["clutch_FtPct"] = S2016_clutch_ft['count_x']/(S2016_clutch_ft['count_x']+ S2016_clutch_ft['count_y'])
S2016_clutch_ft['TeamID'] = S2016_clutch_ft['WTeamID']
S2016_clutch_ft = S2016_clutch_ft[['TeamID','clutch_FtPct']]

#Look at momentum as well (weight later wins more)
#Conference Record
sql140 = '''
SELECT "WTeamID",COUNT("WTeamID") FROM "ConferenceTourneyGames"
WHERE "Season" = 2016
GROUP BY "WTeamID"
ORDER BY COUNT("WTeamID") DESC
'''
S2016_conference_wins = pd.read_sql(sql140, conn)
#conference_wins['TeamID'] = conference_wins['WTeamID']
sql141 = '''
SELECT "LTeamID",COUNT("LTeamID") FROM "ConferenceTourneyGames"
WHERE "Season" = 2016
GROUP BY "LTeamID"
ORDER BY COUNT("LTeamID") DESC
'''
S2016_conference_losses = pd.read_sql(sql141, conn)
#conference_losses['TeamID'] = conference_losses["LTeamID"]

S2016_conference_record = pd.merge(S2016_conference_wins,S2016_conference_losses,how = 'left', left_on = "WTeamID", right_on = "LTeamID")
S2016_conference_record.fillna(0, inplace = True)
S2016_conference_record['conf_win_pct'] = S2016_conference_record['count_x']/(S2016_conference_record['count_x'] + S2016_conference_record['count_y'])
S2016_conference_record['TeamID'] = S2016_conference_record['WTeamID']
S2016_conference_record=S2016_conference_record[['TeamID','conf_win_pct']]

#combine inputs
#total_avg_pts - total points they got on average TeamID, total_avg_pts['ppg']
#points_scored_win-points scored in a win
#total_avg_pa - total average points given up TeamID, total_avg_pa['ppg']
#home court- home court advantage  WTeamID, home_court['ratio']
#W_strength_of_schedule - strength of schedules of wins   
#L_strength_of_schedule - strength of schedules of losses
#coach record- success of coaches
#clutch_ft - last 2 minutes while winning field goals made 
#o_rbs_for- average offensive rebounds for
#o_rbs_against-average offensive rebounds against
#to_made =  turnover done (ie while on defense)
#to_against = when they turn it over
#conference record-how they did in the conference tourney
#seed_l: seed of 2016 tourney "TeamID", seel_d["Seed"]

#put everything together
#join everything by 'TeamID'
S2016_overall_df = pd.merge(S2016_total_avg_pts,S2016_total_avg_pa, how = 'left',on = 'TeamID').merge(S2016_home_record, how = 'left',on = 'TeamID').merge(S2016_away_record,how = 'left',on = 'TeamID').merge(
        S2016_W_strength_of_schedule,how = 'left',on = 'TeamID').merge(S2016_L_strength_of_schedule, how = 'left',on = 'TeamID').merge(S2016_team_coach_record,how = 'left',on = 'TeamID').merge(
        S2016_clutch_ft, how = 'left',on = 'TeamID').merge(S2016_o_rbs_for, how = 'left',on = 'TeamID').merge(S2016_o_rbs_against, how = 'left',on = 'TeamID').merge(S2016_to_made, how = 'left',on = 'TeamID').merge(
        S2016_to_against, how = 'left',on = 'TeamID').merge(S2016_conference_record,how = 'left', on = 'TeamID').merge(S2016_seed,how = 'left', on = 'TeamID')