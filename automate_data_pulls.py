# -*- coding: utf-8 -*-
"""
Created on Wed Dec 26 12:26:20 2018

@author: micha
"""
import pickle
import pandas as pd
import psycopg2 as pg2
from sqlalchemy import create_engine
import numpy as np

conn = pg2.connect(database = 'March_Madness' , user = 'postgres', password = 'crump83')
engine = create_engine('postgresql://postgres:crump83@localhost:5432/March_Madness')
cur = conn.cursor()

sql_tourney = r'''
    SELECT * FROM "March Madness 18 Tourney"
    '''
get_seed = pd.read_sql(sql_tourney, conn)


sql001 = '''
SELECT * FROM "Teams" '''
df_teams = pd.read_sql(sql001,conn)


#figure out who won the most
#2016 analysis

#regular season success
def datapull(b):
    

    sql170 = '''
SELECT "TeamID", "Seed","Season" FROM "NCAATourneySeeds"
'''


    auto_seed2 = pd.read_sql(sql170,conn)
    auto_seed2 = auto_seed2[auto_seed2['Season'] == b]
    auto_seed2 = auto_seed2[['TeamID','Seed']]
    

#Average Team Stats (add rebounds, turnovers (both winner and loser), etc)
#WTO are offensive players


#do overall points (both winning and losing)
    sql125 = '''
SELECT "WTeamID", "WScore","Season"
FROM "RegularSeasonDetailedResults"

'''

    auto_total_wpts = pd.read_sql(sql125,conn)
    auto_total_wpts = auto_total_wpts[auto_total_wpts['Season'] == b]
    auto_total_wpts1= auto_total_wpts.groupby(['WTeamID'], as_index = False)['WScore'].sum()

#auto_total_wpts1.reset_index(inplace = True)
    auto_total_wpts1.columns = ['TeamID','sum_x']
    auto_total_wpts2 =  auto_total_wpts.groupby(['WTeamID'], as_index = False)['WScore'].count()
#auto_total_wpts1.reset_index(inplace = True)
    auto_total_wpts2.columns = ['TeamID','count_x']

    auto_total_wpts3 = pd.merge(auto_total_wpts1, auto_total_wpts2, on = 'TeamID')
#auto_total_wpts['TeamID'] = auto_total_wpts['WTeamID']
#losing sum/count

    sql126 = '''
SELECT "LTeamID", "LScore", "Season"
FROM "RegularSeasonDetailedResults"

'''
    auto_total_lpts = pd.read_sql(sql126, conn)
    auto_total_lpts = auto_total_lpts[auto_total_lpts['Season'] == b]
    auto_total_lpts1= auto_total_lpts.groupby(['LTeamID'], as_index = False)['LScore'].sum()
    auto_total_lpts1.columns = ['TeamID','sum_y']
    auto_total_lpts2 =  auto_total_lpts.groupby(['LTeamID'], as_index = False)['LScore'].count()
    auto_total_lpts2.columns = ['TeamID','count_y']
    auto_total_lpts3 = pd.merge(auto_total_lpts1, auto_total_lpts2, on = 'TeamID')


    auto_total_avg_pts = pd.merge(auto_total_wpts3, auto_total_lpts3, how = 'inner', on = 'TeamID')
    auto_total_avg_pts['total_pts'] = auto_total_avg_pts['sum_x'] + auto_total_avg_pts['sum_y']
    auto_total_avg_pts['total_games'] = auto_total_avg_pts['count_x'] + auto_total_avg_pts['count_y']

    auto_total_avg_pts['Average Pts'] = auto_total_avg_pts['total_pts']/auto_total_avg_pts['total_games']
    auto_total_avg_pts = auto_total_avg_pts[['TeamID','Average Pts']]
#Points allowed
    b = 2017
    sql127 = '''
SELECT "WTeamID", "LScore","Season"
FROM "RegularSeasonDetailedResults"
'''
    auto_points_allowed_w = pd.read_sql(sql127,conn)
    auto_points_allowed_w = auto_points_allowed_w[auto_points_allowed_w['Season'] == b]
    auto_points_allowed_w1= auto_points_allowed_w.groupby(['WTeamID'], as_index = False)['LScore'].sum()
    auto_points_allowed_w1.columns = ['TeamID','sum_x']
    auto_points_allowed_w2 =  auto_points_allowed_w.groupby(['WTeamID'], as_index = False)['LScore'].count()
    auto_points_allowed_w2.columns = ['TeamID','count_x']
    auto_points_allowed_w3 = pd.merge(auto_points_allowed_w1, auto_points_allowed_w2, on = 'TeamID')

    sql128 = '''
SELECT "LTeamID", "WScore", "Season"
FROM "RegularSeasonDetailedResults"

'''
    auto_points_allowed_l = pd.read_sql(sql128,conn)
    auto_points_allowed_l = auto_points_allowed_l[auto_points_allowed_l['Season'] == b]
    auto_points_allowed_l1= auto_points_allowed_l.groupby(['LTeamID'], as_index = False)['WScore'].sum()
    auto_points_allowed_l1.columns = ['TeamID','sum_y']
    auto_points_allowed_l2 =  auto_points_allowed_l.groupby(['LTeamID'], as_index = False)['WScore'].count()
    auto_points_allowed_l2.columns = ['TeamID','count_y']
    auto_points_allowed_l3 = pd.merge(auto_points_allowed_l1, auto_points_allowed_l2, on = 'TeamID')
#12/26-left off here
    auto_total_avg_pa = pd.merge(auto_points_allowed_w3, auto_points_allowed_l3, on = 'TeamID')
    auto_total_avg_pa['total_pts'] = auto_total_avg_pa['sum_x'] + auto_total_avg_pa['sum_y']
    auto_total_avg_pa['total_games'] = auto_total_avg_pa['count_x'] + auto_total_avg_pa['count_y']

    auto_total_avg_pa['Average PA'] = auto_total_avg_pa['total_pts']/auto_total_avg_pa['total_games']
    auto_total_avg_pa = auto_total_avg_pa[['TeamID','Average PA']]

#join winning and losing sum columns and counts
#divide out


#ORB
    sql129 = '''
SELECT "WTeamID", "WOR","Season"
FROM "RegularSeasonDetailedResults"

'''
    auto_o_rbs_w = pd.read_sql(sql129, conn)
    auto_o_rbs_w = auto_o_rbs_w[auto_o_rbs_w['Season'] == b]
    auto_o_rbs_w1= auto_o_rbs_w.groupby(['WTeamID'], as_index = False)['WOR'].sum()
    auto_o_rbs_w1.columns = ['TeamID','sum_x']
    auto_o_rbs_w2 =  auto_o_rbs_w.groupby(['WTeamID'], as_index = False)['WOR'].count()
    auto_o_rbs_w2.columns = ['TeamID','count_x']
    auto_o_rbs_w3 = pd.merge(auto_o_rbs_w1, auto_o_rbs_w2, on = 'TeamID')
    
    sql130 = '''
SELECT "LTeamID", "LOR","Season"
FROM "RegularSeasonDetailedResults"

'''

    auto_o_rbs_l = pd.read_sql(sql130, conn)
    auto_o_rbs_l = auto_o_rbs_l[auto_o_rbs_l['Season'] == b]
    auto_o_rbs_l1= auto_o_rbs_l.groupby(['LTeamID'], as_index = False)['LOR'].sum()
    auto_o_rbs_l1.columns = ['TeamID','sum_y']
    auto_o_rbs_l2 =  auto_o_rbs_l.groupby(['LTeamID'], as_index = False)['LOR'].count()
    auto_o_rbs_l2.columns = ['TeamID','count_y']
    auto_o_rbs_l3 = pd.merge(auto_o_rbs_l1, auto_o_rbs_l2, on = 'TeamID')
    auto_o_rbs_for = pd.merge(auto_o_rbs_w3,auto_o_rbs_l3, how = 'inner', on = 'TeamID')
    auto_o_rbs_for['total_rbs'] = auto_o_rbs_for['sum_x'] + auto_o_rbs_for['sum_y']
    auto_o_rbs_for['total_games'] = auto_o_rbs_for['count_x'] + auto_o_rbs_for['count_y']
    auto_o_rbs_for['orpg'] = auto_o_rbs_for['total_rbs']/auto_o_rbs_for['total_games']
    auto_o_rbs_for = auto_o_rbs_for[['TeamID','orpg']]

#offensive rebounds allowed
    sql131 = '''
SELECT "WTeamID", "LOR","Season"
FROM "RegularSeasonDetailedResults"

'''
    auto_o_rbs_a_w = pd.read_sql(sql131, conn)
    auto_o_rbs_a_w = auto_o_rbs_a_w[auto_o_rbs_a_w['Season'] == b]
    auto_o_rbs_a_w1 = auto_o_rbs_a_w.groupby(['WTeamID'], as_index = False)['LOR'].sum()
    auto_o_rbs_a_w1.columns = ['TeamID','sum_x']
    auto_o_rbs_a_w2 =  auto_o_rbs_a_w.groupby(['WTeamID'], as_index = False)['LOR'].count()
    auto_o_rbs_a_w2.columns = ['TeamID','count_x']
    auto_o_rbs_a_w3 = pd.merge(auto_o_rbs_a_w1, auto_o_rbs_a_w2, on = 'TeamID')


    sql132 = '''
SELECT "LTeamID", "WOR", "Season"
FROM "RegularSeasonDetailedResults"

'''
    auto_o_rbs_a_l = pd.read_sql(sql132,conn)
    auto_o_rbs_a_l = auto_o_rbs_a_l[auto_o_rbs_a_l['Season'] == b]
    auto_o_rbs_a_l1 = auto_o_rbs_a_l.groupby(['LTeamID'], as_index = False)['WOR'].sum()
    auto_o_rbs_a_l1.columns = ['TeamID','sum_y']
    auto_o_rbs_a_l2 =  auto_o_rbs_a_l.groupby(['LTeamID'], as_index = False)['WOR'].count()
    auto_o_rbs_a_l2.columns = ['TeamID','count_y']
    auto_o_rbs_a_l3 = pd.merge(auto_o_rbs_a_l1, auto_o_rbs_a_l2, on = 'TeamID')
    auto_o_rbs_against = pd.merge(auto_o_rbs_a_w3,auto_o_rbs_a_l3, how = 'inner', on = 'TeamID')
    auto_o_rbs_against['total_rb_a'] = auto_o_rbs_against['sum_x'] + auto_o_rbs_against['sum_y']
    auto_o_rbs_against['total_games'] = auto_o_rbs_against['count_x'] + auto_o_rbs_against['count_y']
    auto_o_rbs_against['orapg'] = auto_o_rbs_against['total_rb_a']/auto_o_rbs_against['total_games']
    auto_o_rbs_against= auto_o_rbs_against[['TeamID','orapg']]

#Do check-look at points for/points against games 


#Turnovers
#turnovers made
    sql133 = '''
SELECT "WTeamID", "LTO", "Season"
FROM "RegularSeasonDetailedResults"

'''
    auto_to_w = pd.read_sql(sql133, conn)

    auto_to_w = auto_to_w[auto_to_w['Season'] == b]
    auto_to_w1= auto_to_w.groupby(['WTeamID'], as_index = False)['LTO'].sum()
    auto_to_w1.columns = ['TeamID','sum_x']
    auto_to_w2 =  auto_to_w.groupby(['WTeamID'], as_index = False)['LTO'].count()
    auto_to_w2.columns = ['TeamID','count_x']
    auto_to_w3 = pd.merge(auto_to_w1, auto_to_w2, on = 'TeamID')
    
    sql134 = '''
SELECT "LTeamID", "WTO","Season"
FROM "RegularSeasonDetailedResults"

'''

    auto_to_l = pd.read_sql(sql134, conn)
    auto_to_l = auto_to_l[auto_to_l['Season'] == b]
    auto_to_l1= auto_to_l.groupby(['LTeamID'], as_index = False)['WTO'].sum()
    auto_to_l1.columns = ['TeamID','sum_y']
    auto_to_l2 =  auto_to_l.groupby(['LTeamID'], as_index = False)['WTO'].count()
    auto_to_l2.columns = ['TeamID','count_y']
    auto_to_l3 = pd.merge(auto_to_l1, auto_to_l2, on = 'TeamID')
    auto_to_made = pd.merge(auto_to_w3,auto_to_l3, how = 'inner', on = 'TeamID')
    auto_to_made['total_tos_made'] = auto_to_made['sum_x'] + auto_to_made['sum_y']
    auto_to_made['total_games'] = auto_to_made['count_x'] + auto_to_made['count_y']
    auto_to_made['avg_to_made'] = auto_to_made['total_tos_made']/auto_to_made['total_games']
    auto_to_made = auto_to_made[['TeamID','avg_to_made']]

#turnovers against
    sql135 = '''
SELECT "WTeamID", "WTO", "Season"
FROM "RegularSeasonDetailedResults"

'''
    auto_to_against_w = pd.read_sql(sql135, conn)
    auto_to_against_w = auto_to_against_w[auto_to_against_w['Season'] == b]
    auto_to_against_w1= auto_to_against_w.groupby(['WTeamID'], as_index = False)['WTO'].sum()
    auto_to_against_w1.columns = ['TeamID','sum_x']
    auto_to_against_w2 =  auto_to_against_w.groupby(['WTeamID'], as_index = False)['WTO'].count()
    auto_to_against_w2.columns = ['TeamID','count_x']
    auto_to_against_w3 = pd.merge(auto_to_against_w1, auto_to_against_w2, on = 'TeamID')

    sql136 = '''
SELECT "LTeamID", "LTO","Season"
FROM "RegularSeasonDetailedResults"

'''

    auto_to_against_l = pd.read_sql(sql136, conn)
    auto_to_against_l = auto_to_against_l[auto_to_against_l['Season'] == b]
    auto_to_against_l1= auto_to_against_l.groupby(['LTeamID'], as_index = False)['LTO'].sum()
    auto_to_against_l1.columns = ['TeamID','sum_y']
    auto_to_against_l2 =  auto_to_against_l.groupby(['LTeamID'], as_index = False)['LTO'].count()
    auto_to_against_l2.columns = ['TeamID','count_y']
    auto_to_against_l3 = pd.merge(auto_to_against_l1, auto_to_against_l2, on = 'TeamID')
    auto_avg_to_against = pd.merge(auto_to_against_w3,auto_to_against_l3, how = 'inner', on = 'TeamID')
    auto_avg_to_against['total_tos_against'] = auto_avg_to_against['sum_x'] + auto_avg_to_against['sum_y']
    auto_avg_to_against['total_games'] = auto_avg_to_against['count_x'] + auto_avg_to_against['count_y']
    auto_avg_to_against['avg_to_allowed'] = auto_avg_to_against['total_tos_against']/auto_avg_to_against['total_games']
    auto_avg_to_against = auto_avg_to_against[['TeamID','avg_to_allowed']]






#winning team home vs losing team home
    sql108 = '''
SELECT "WTeamID", "WLoc", "Season" FROM "RegularSeasonDetailedResults"

'''

    sql110 = '''
SELECT "WTeamID", "WLoc","Season" FROM "RegularSeasonDetailedResults"

;
'''

    sql160='''
SELECT "LTeamID", "WLoc", "Season" FROM "RegularSeasonDetailedResults"

;
'''
    sql161 = '''
SELECT "LTeamID", "WLoc", "Season" FROM "RegularSeasonDetailedResults"

'''


    auto_home_wins = pd.read_sql(sql108, conn)
    auto_home_losses = pd.read_sql(sql160, conn)
    auto_away_wins = pd.read_sql(sql110, conn) 
    auto_away_losses = pd.read_sql(sql161, conn)

    auto_home_wins = auto_home_wins[auto_home_wins['Season'] == b]
    auto_home_wins = auto_home_wins[auto_home_wins['WLoc'] == 'H']
    auto_home_wins= auto_home_wins.groupby(['WTeamID'], as_index = False).count()
    auto_home_wins = auto_home_wins[['WTeamID',"WLoc"]]
    auto_home_wins.columns = ['TeamID','Home Wins']

    auto_home_losses = auto_home_losses[auto_home_losses['Season'] == b]
    auto_home_losses = auto_home_losses[auto_home_losses['WLoc'] == 'A']
    auto_home_losses = auto_home_losses.groupby(['LTeamID'], as_index = False).count()
    auto_home_losses = auto_home_losses[['LTeamID',"WLoc"]]
    auto_home_losses.columns = ['TeamID','Home Losses']

    auto_away_wins= auto_away_wins[auto_away_wins['Season'] == b]
    auto_away_wins= auto_away_wins[auto_away_wins['WLoc'] == 'A']
    auto_away_wins= auto_away_wins.groupby(['WTeamID'], as_index = False).count()
    auto_away_wins = auto_away_wins[['WTeamID',"WLoc"]]
    auto_away_wins.columns = ['TeamID','Away Wins']

    auto_away_losses= auto_away_losses[auto_away_losses['Season'] == b]
    auto_away_losses= auto_away_losses[auto_away_losses['WLoc'] == 'H']
    auto_away_losses= auto_away_losses.groupby(['LTeamID'], as_index = False).count()
    auto_away_losses = auto_away_losses[['LTeamID',"WLoc"]]
    auto_away_losses.columns = ['TeamID','Away Losses']

    auto_home_record = pd.merge(auto_home_wins, auto_home_losses, on = "TeamID")
    auto_home_record['Home Record'] = auto_home_record['Home Wins']/(auto_home_record['Home Wins'] + auto_home_record['Home Losses'])
    auto_home_record = auto_home_record[['TeamID','Home Record']]

    auto_away_record = pd.merge(auto_away_wins, auto_away_losses, on = "TeamID")
    auto_away_record['Away Record'] = auto_away_record['Away Wins']/(auto_away_record['Away Wins'] + auto_away_record['Away Losses'])
    auto_away_record= auto_away_record[['TeamID','Away Record']]

#do home record, away record
#build a function looking at location, only include the one that applies (ie away record if team is away)
    sql140 = '''
SELECT "WTeamID","Season" FROM "ConferenceTourneyGames"

'''
    auto_conference_wins = pd.read_sql(sql140, conn)
    auto_conference_wins = auto_conference_wins[auto_conference_wins['Season'] == b]
    auto_conference_wins= auto_conference_wins.groupby(['WTeamID'], as_index = False).count()
    auto_conference_wins.columns = ['TeamID','Wins']

    sql141 = '''
SELECT "LTeamID","Season" FROM "ConferenceTourneyGames"

'''
    auto_conference_losses = pd.read_sql(sql141, conn)
    auto_conference_losses = auto_conference_losses[auto_conference_losses['Season'] == b]
    auto_conference_losses= auto_conference_losses.groupby(['LTeamID'], as_index = False).count()
    auto_conference_losses.columns = ['TeamID','Losses']
#conference_losses['TeamID'] = conference_losses["LTeamID"]

    auto_conference_record = pd.merge(auto_conference_wins,auto_conference_losses, on = "TeamID")
    auto_conference_record['conf_win_pct'] = auto_conference_record['Wins']/(auto_conference_record['Wins'] + auto_conference_record['Losses'])
    auto_conference_record = auto_conference_record[['TeamID','conf_win_pct']]
    #Coach Analysis
#Total Tourney Wins/Losses for Coaches
    sql116 = r'''
SELECT "TeamCoaches"."CoachName",COUNT("NCAATourneyDetailedResults"."WTeamID")
FROM "TeamCoaches"
INNER JOIN "NCAATourneyDetailedResults"  ON "TeamCoaches"."TeamID" = "NCAATourneyDetailedResults"."WTeamID" AND "TeamCoaches"."Season" = "NCAATourneyDetailedResults"."Season"  
GROUP BY "CoachName"
ORDER BY "count" DESC
'''
    coach_wins = pd.read_sql(sql116, conn)

    sql117 = r'''
SELECT "TeamCoaches"."CoachName",COUNT("NCAATourneyDetailedResults"."LTeamID")
FROM "TeamCoaches"
INNER JOIN "NCAATourneyDetailedResults"  ON "TeamCoaches"."TeamID" = "NCAATourneyDetailedResults"."LTeamID" AND "TeamCoaches"."Season" = "NCAATourneyDetailedResults"."Season"  
GROUP BY "CoachName"
ORDER BY "count" DESC
'''
    coach_losses = pd.read_sql(sql117, conn)

    coach_record = pd.merge(coach_wins,coach_losses, on = "CoachName")
    coach_record["coach_win_pct"] = coach_record["count_x"]/(coach_record["count_x"]+ coach_record["count_y"]) 
    sql71 = r'''
SELECT * FROM "TeamCoaches" 


'''
    auto_team_coach = pd.read_sql(sql71, conn)
    auto_team_coach = auto_team_coach[auto_team_coach['Season'] == b]
    
    auto_team_coach_record = pd.merge(coach_record, auto_team_coach, on = "CoachName")
    auto_team_coach_record = auto_team_coach_record[['TeamID','coach_win_pct']]
    
    #combine everything (SoS and ft% remaining)
    auto_overall_df = pd.merge(auto_total_avg_pts,auto_total_avg_pa, how = 'left',on = 'TeamID').merge(auto_home_record, how = 'left',on = 'TeamID').merge(auto_away_record,how = 'left',on = 'TeamID').merge(
    auto_team_coach_record,how = 'left',on = 'TeamID').merge(
   auto_o_rbs_for, how = 'left',on = 'TeamID').merge(auto_o_rbs_against, how = 'left',on = 'TeamID').merge(
   auto_to_made, how = 'left',on = 'TeamID').merge(
   auto_avg_to_against, how = 'left',on = 'TeamID').merge(auto_conference_record,how = 'left', on = 'TeamID').merge(auto_seed2,how = 'left', on = 'TeamID')
   
    auto_overall_df["conf_win_pct"].fillna(0, inplace = True)
    auto_overall_df["coach_win_pct"].fillna(auto_overall_df["coach_win_pct"].mean(), inplace = True)
    auto_overall_df["Home Record"].fillna(auto_overall_df["Home Record"].mean(), inplace = True)
    auto_overall_df["Away Record"].fillna(auto_overall_df["Away Record"].mean(), inplace = True)
    auto_overall_df['Seed'] = auto_overall_df['Seed'].str[1:3]
    auto_overall_df['Seed'].fillna(0, inplace = True)
    auto_overall_df['Seed'] = auto_overall_df['Seed'].astype(int) 
    return auto_overall_df

def remaining_data():
    FY2010_df = datapull(2010)
    FY2011_df = datapull(2011)
    FY2012_df = datapull(2012)
    FY2013_df = datapull(2013)
    FY2014_df = datapull(2014)
    FY2015_df = datapull(2015)
    FY2016_df = datapull(2016)
    FY2017_df = datapull(2017)
    
    #add in SoS
    #2010
    sql114 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2010 Record"."TeamID","2010 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2010 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2010 Record"."TeamID"
WHERE "Season" = 2010) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 
    FY2010_sos_w = pd.read_sql(sql114, conn)
    FY2010_sos_w.columns = ['TeamID','WSoS']
    FY2010_df_2 = pd.merge(FY2010_df, FY2010_sos_w, how = 'left',on = 'TeamID')

    sql115 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2010 Record"."TeamID","2010 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2010 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2010 Record"."TeamID"
WHERE "Season" = 2010) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
''' 

    FY2010_sos_l = pd.read_sql(sql115, conn)
    FY2010_sos_l.columns = ['TeamID','LSoS']
    FY2010_df_3 = pd.merge(FY2010_df_2, FY2010_sos_l, how = 'left',on = 'TeamID')
    
    #2011
    sql116 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2011 Record"."TeamID","2011 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2011 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2011 Record"."TeamID"
WHERE "Season" = 2011) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 
    FY2011_sos_w = pd.read_sql(sql116, conn)
    FY2011_sos_w.columns = ['TeamID','WSoS']
    FY2011_df_2 = pd.merge(FY2011_df, FY2011_sos_w, how = 'left',on = 'TeamID')
    
    sql117 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2011 Record"."TeamID","2011 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2011 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2011 Record"."TeamID"
WHERE "Season" = 2011) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
'''

    FY2011_sos_l = pd.read_sql(sql117, conn)
    FY2011_sos_l.columns = ['TeamID','LSoS']
    FY2011_df_3 = pd.merge(FY2011_df_2, FY2011_sos_l, how = 'left',on = 'TeamID')
    #2012
    
    sql118 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2012 Record"."TeamID","2012 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2012 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2012 Record"."TeamID"
WHERE "Season" = 2012) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 
    FY2012_sos_w = pd.read_sql(sql118, conn)
    FY2012_sos_w.columns = ['TeamID','WSoS']
    FY2012_df_2 = pd.merge(FY2012_df, FY2012_sos_w, how = 'left',on = 'TeamID')
    
    sql119 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2012 Record"."TeamID","2012 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2012 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2012 Record"."TeamID"
WHERE "Season" = 2012) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
'''

    FY2012_sos_l = pd.read_sql(sql119, conn)
    FY2012_sos_l.columns = ['TeamID','LSoS']
    FY2012_df_3 = pd.merge(FY2012_df_2, FY2012_sos_l, how = 'left',on = 'TeamID')
    
    sql120 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2013 Record"."TeamID","2013 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2013 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2013 Record"."TeamID"
WHERE "Season" = 2013) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 
    FY2013_sos_w = pd.read_sql(sql120, conn)
    FY2013_sos_w.columns = ['TeamID','WSoS']
    FY2013_df_2 = pd.merge(FY2013_df, FY2013_sos_w, how = 'left',on = 'TeamID')
    
    sql121 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2013 Record"."TeamID","2013 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2013 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2013 Record"."TeamID"
WHERE "Season" = 2013) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
'''

    FY2013_sos_l = pd.read_sql(sql121, conn)
    FY2013_sos_l.columns = ['TeamID','LSoS']
    FY2013_df_3 = pd.merge(FY2013_df_2, FY2013_sos_l, how = 'left',on = 'TeamID')
    #2014
    sql122 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2014 Record"."TeamID","2014 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2014 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2014 Record"."TeamID"
WHERE "Season" = 2014) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 
    FY2014_sos_w = pd.read_sql(sql122, conn)
    FY2014_sos_w.columns = ['TeamID','WSoS']
    FY2014_df_2 = pd.merge(FY2014_df, FY2014_sos_w, how = 'left',on = 'TeamID')
    
    sql123 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2014 Record"."TeamID","2014 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2014 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2014 Record"."TeamID"
WHERE "Season" = 2014) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
'''

    FY2014_sos_l = pd.read_sql(sql123, conn)
    FY2014_sos_l.columns = ['TeamID','LSoS']
    FY2014_df_3 = pd.merge(FY2014_df_2, FY2014_sos_l, how = 'left',on = 'TeamID')
    
    #2015
    sql124 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2015 Record"."TeamID","2015 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2015 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2015 Record"."TeamID"
WHERE "Season" = 2015) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 
    FY2015_sos_w = pd.read_sql(sql124, conn)
    FY2015_sos_w.columns = ['TeamID','WSoS']
    FY2015_df_2 = pd.merge(FY2015_df, FY2015_sos_w, how = 'left',on = 'TeamID')
    
    sql125 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2015 Record"."TeamID","2015 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2015 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2015 Record"."TeamID"
WHERE "Season" = 2015) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
'''

    FY2015_sos_l = pd.read_sql(sql125, conn)
    FY2015_sos_l.columns = ['TeamID','LSoS']
    FY2015_df_3 = pd.merge(FY2015_df_2, FY2015_sos_l, how = 'left',on = 'TeamID')
    
    #2016
    sql126 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2016 Record"."TeamID","2016 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2016 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2016 Record"."TeamID"
WHERE "Season" = 2016) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 
    FY2016_sos_w = pd.read_sql(sql126, conn)
    FY2016_sos_w.columns = ['TeamID','WSoS']
    FY2016_df_2 = pd.merge(FY2016_df, FY2016_sos_w, how = 'left',on = 'TeamID')
    
    sql127 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2016 Record"."TeamID","2016 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2016 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2016 Record"."TeamID"
WHERE "Season" = 2016) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
'''

    FY2016_sos_l = pd.read_sql(sql127, conn)
    FY2016_sos_l.columns = ['TeamID','LSoS']
    FY2016_df_3 = pd.merge(FY2016_df_2, FY2016_sos_l, how = 'left',on = 'TeamID')
    
    #2017
    sql128 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2017 Record"."TeamID","2017 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2017 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2017 Record"."TeamID"
WHERE "Season" = 2017) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 
    FY2017_sos_w = pd.read_sql(sql128, conn)
    FY2017_sos_w.columns = ['TeamID','WSoS']
    FY2017_df_2 = pd.merge(FY2017_df, FY2017_sos_w, how = 'left',on = 'TeamID')
    
    sql129 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2017 Record"."TeamID","2017 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2017 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2017 Record"."TeamID"
WHERE "Season" = 2017) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
'''

    FY2017_sos_l = pd.read_sql(sql129, conn)
    FY2017_sos_l.columns = ['TeamID','LSoS']
    FY2017_df_3 = pd.merge(FY2017_df_2, FY2017_sos_l, how = 'left',on = 'TeamID')
    
    
    
    
    
    #add in FT%
    sql201 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents17"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

    FY2017_clutch_ft_made = pd.read_sql(sql201, conn)

    sql202 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents17"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
    FY2017_clutch_ft_miss = pd.read_sql(sql202, conn)

    FY2017_clutch_ft = pd.merge(FY2017_clutch_ft_made,FY2017_clutch_ft_miss, on = "WTeamID")
    FY2017_clutch_ft["clutch_FtPct"] = FY2017_clutch_ft['count_x']/(FY2017_clutch_ft['count_x']+ FY2017_clutch_ft['count_y'])
    FY2017_clutch_ft['TeamID'] = FY2017_clutch_ft['WTeamID']
    FY2017_clutch_ft = FY2017_clutch_ft[['TeamID','clutch_FtPct']]
    FY2017_df_4 = pd.merge(FY2017_df_3, FY2017_clutch_ft, how = 'left',on = 'TeamID')
    
    #2016
    sql203 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents16"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

    FY2016_clutch_ft_made = pd.read_sql(sql203, conn)

    sql204 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents16"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
    FY2016_clutch_ft_miss = pd.read_sql(sql204, conn)

    FY2016_clutch_ft = pd.merge(FY2016_clutch_ft_made,FY2016_clutch_ft_miss, on = "WTeamID")
    FY2016_clutch_ft["clutch_FtPct"] = FY2016_clutch_ft['count_x']/(FY2016_clutch_ft['count_x']+ FY2016_clutch_ft['count_y'])
    FY2016_clutch_ft['TeamID'] = FY2016_clutch_ft['WTeamID']
    FY2016_clutch_ft = FY2016_clutch_ft[['TeamID','clutch_FtPct']]
    FY2016_df_4 = pd.merge(FY2016_df_3, FY2016_clutch_ft, how = 'left',on = 'TeamID')

    #2015
    sql205 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents15"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

    FY2015_clutch_ft_made = pd.read_sql(sql205, conn)

    sql206 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents15"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
    FY2015_clutch_ft_miss = pd.read_sql(sql206, conn)

    FY2015_clutch_ft = pd.merge(FY2015_clutch_ft_made,FY2015_clutch_ft_miss, on = "WTeamID")
    FY2015_clutch_ft["clutch_FtPct"] = FY2015_clutch_ft['count_x']/(FY2015_clutch_ft['count_x']+ FY2015_clutch_ft['count_y'])
    FY2015_clutch_ft['TeamID'] = FY2015_clutch_ft['WTeamID']
    FY2015_clutch_ft = FY2015_clutch_ft[['TeamID','clutch_FtPct']]
    FY2015_df_4 = pd.merge(FY2015_df_3, FY2015_clutch_ft, how = 'left',on = 'TeamID')
    
    #2014
    sql207 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents14"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

    FY2014_clutch_ft_made = pd.read_sql(sql207, conn)

    sql208 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents14"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
    FY2014_clutch_ft_miss = pd.read_sql(sql208, conn)

    FY2014_clutch_ft = pd.merge(FY2014_clutch_ft_made,FY2014_clutch_ft_miss, on = "WTeamID")
    FY2014_clutch_ft["clutch_FtPct"] = FY2014_clutch_ft['count_x']/(FY2014_clutch_ft['count_x']+ FY2014_clutch_ft['count_y'])
    FY2014_clutch_ft['TeamID'] = FY2014_clutch_ft['WTeamID']
    FY2014_clutch_ft = FY2014_clutch_ft[['TeamID','clutch_FtPct']]
    FY2014_df_4 = pd.merge(FY2014_df_3, FY2014_clutch_ft, how = 'left',on = 'TeamID')
    
    #2013
    sql209 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents13"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

    FY2013_clutch_ft_made = pd.read_sql(sql209, conn)

    sql210 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents13"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
    FY2013_clutch_ft_miss = pd.read_sql(sql210, conn)

    FY2013_clutch_ft = pd.merge(FY2013_clutch_ft_made,FY2013_clutch_ft_miss, on = "WTeamID")
    FY2013_clutch_ft["clutch_FtPct"] = FY2013_clutch_ft['count_x']/(FY2013_clutch_ft['count_x']+ FY2013_clutch_ft['count_y'])
    FY2013_clutch_ft['TeamID'] = FY2013_clutch_ft['WTeamID']
    FY2013_clutch_ft = FY2013_clutch_ft[['TeamID','clutch_FtPct']]
    FY2013_df_4 = pd.merge(FY2013_df_3, FY2013_clutch_ft, how = 'left',on = 'TeamID')
    
    #2012
    sql211 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents12"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

    FY2012_clutch_ft_made = pd.read_sql(sql211, conn)

    sql212 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents12"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
    FY2012_clutch_ft_miss = pd.read_sql(sql212, conn)

    FY2012_clutch_ft = pd.merge(FY2012_clutch_ft_made,FY2012_clutch_ft_miss, on = "WTeamID")
    FY2012_clutch_ft["clutch_FtPct"] = FY2012_clutch_ft['count_x']/(FY2012_clutch_ft['count_x']+ FY2012_clutch_ft['count_y'])
    FY2012_clutch_ft['TeamID'] = FY2012_clutch_ft['WTeamID']
    FY2012_clutch_ft = FY2012_clutch_ft[['TeamID','clutch_FtPct']]
    FY2012_df_4 = pd.merge(FY2012_df_3, FY2012_clutch_ft, how = 'left',on = 'TeamID')
    
    #2011
    sql211 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents11"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

    FY2011_clutch_ft_made = pd.read_sql(sql211, conn)

    sql212 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents11"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
    FY2011_clutch_ft_miss = pd.read_sql(sql212, conn)

    FY2011_clutch_ft = pd.merge(FY2011_clutch_ft_made,FY2011_clutch_ft_miss, on = "WTeamID")
    FY2011_clutch_ft["clutch_FtPct"] = FY2011_clutch_ft['count_x']/(FY2011_clutch_ft['count_x']+ FY2011_clutch_ft['count_y'])
    FY2011_clutch_ft['TeamID'] = FY2011_clutch_ft['WTeamID']
    FY2011_clutch_ft = FY2011_clutch_ft[['TeamID','clutch_FtPct']]
    FY2011_df_4 = pd.merge(FY2011_df_3, FY2011_clutch_ft, how = 'left',on = 'TeamID')
    
    #2010
    sql213 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents10"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

    FY2010_clutch_ft_made = pd.read_sql(sql211, conn)

    sql214 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents10"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
    FY2010_clutch_ft_miss = pd.read_sql(sql214, conn)

    FY2010_clutch_ft = pd.merge(FY2010_clutch_ft_made,FY2010_clutch_ft_miss, on = "WTeamID")
    FY2010_clutch_ft["clutch_FtPct"] = FY2010_clutch_ft['count_x']/(FY2010_clutch_ft['count_x']+ FY2010_clutch_ft['count_y'])
    FY2010_clutch_ft['TeamID'] = FY2010_clutch_ft['WTeamID']
    FY2010_clutch_ft = FY2010_clutch_ft[['TeamID','clutch_FtPct']]
    FY2010_df_4 = pd.merge(FY2010_df_3, FY2010_clutch_ft, how = 'left',on = 'TeamID')
    
    FY2017_df_4['Season'] = 2017
    FY2016_df_4['Season'] = 2016
    FY2015_df_4['Season'] = 2015
    FY2014_df_4['Season'] = 2014
    FY2013_df_4['Season'] = 2013
    FY2012_df_4['Season'] = 2012
    FY2011_df_4['Season'] = 2011
    FY2010_df_4['Season'] = 2010
    Final_every_year = FY2017_df_4.append([FY2016_df_4,FY2015_df_4,FY2014_df_4,FY2013_df_4,FY2012_df_4,FY2011_df_4,FY2010_df_4])
    return Final_every_year

def set_up_winner():
    df = remaining_data()
    sql303 = r'''

SELECT "WTeamID","LTeamID", "Season" FROM "NCAATourneyCompactResults"
WHERE "Season" between 2010 and 2017



'''
#add in something about season
    winner = pd.read_sql(sql303, conn)


#randomize data
    import random
    a,b = winner.shape


    winner['Rand'] = random.random()


    for i in range(a):
        winner.iloc[i,b] = random.random()

    winner['Team1'] = 3
    winner['Team2'] = 4     

    winner['Output'] = 2
    c = b+1
    d = b+2
    e = b+3


    for j in range(a):
        if winner.iloc[j,b] < 0.5:
            winner.iloc[j,c] = winner.iloc[j,0]
            winner.iloc[j,d] = winner.iloc[j,1]
            winner.iloc[j,e] = 1

        if winner.iloc[j,b] >= .5:
            winner.iloc[j,c] = winner.iloc[j,1]
            winner.iloc[j,d] = winner.iloc[j,0]
            winner.iloc[j,e] = 0
        
    winner2 = winner[['Team1','Team2','Output','Season']]
   #new_df = pd.merge(A_df, B_df,  how='left', left_on='[A_c1,c2]', right_on = '[B_c1,c2]')     
    auto_final_df_left = pd.merge(df,winner2, left_on = ['TeamID', 'Season'],right_on = ['Team1','Season'])
    auto_final_df = pd.merge(auto_final_df_left, df, left_on = ['Team2','Season'], right_on = ['TeamID','Season'])
    from sklearn.utils import shuffle
    auto_final_df = shuffle(auto_final_df)
    return auto_final_df
    
def set_up_model():
    dataframe = set_up_winner()
 #   'TeamID_x', 'Average Pts_x', 'Average PA_x', 'Home Record_x',
 #      'Away Record_x', 'coach_win_pct_x', 'orpg_x', 'orapg_x',
 #      'avg_to_made_x', 'avg_to_allowed_x', 'conf_win_pct_x', 'Seed_x',
 #      'WSoS_x', 'LSoS_x', 'clutch_FtPct_x', 'Season', 'Team1', 'Team2',
 #      'Output', 'TeamID_y', 'Average Pts_y', 'Average PA_y', 'Home Record_y',
 #      'Away Record_y', 'coach_win_pct_y', 'orpg_y', 'orapg_y',
 #      'avg_to_made_y', 'avg_to_allowed_y', 'conf_win_pct_y', 'Seed_y',
 #      'WSoS_y', 'LSoS_y', 'clutch_FtPct_y']
    #Set up new columns:
    dataframe['Average Pts'] = dataframe['Average Pts_x'] - dataframe['Average Pts_y']
    dataframe['Average PA'] = dataframe['Average PA_x'] - dataframe['Average PA_y']
    dataframe['Home Record'] = dataframe['Home Record_x'] - dataframe['Home Record_y']
    dataframe['Away Record'] = dataframe['Away Record_x'] -dataframe['Away Record_y']
    dataframe['Coach Record'] = dataframe['coach_win_pct_x'] - dataframe['coach_win_pct_y']
    dataframe['offensive rebounds'] = dataframe['orpg_x'] - dataframe['orpg_y']
    dataframe['offensive rbs allowed'] = dataframe['orapg_x'] - dataframe['orapg_y']
    dataframe['avg_to_made'] = dataframe['avg_to_made_x'] - dataframe['avg_to_made_y']
    dataframe['avg_to_allowed'] = dataframe['avg_to_allowed_x'] - dataframe['avg_to_allowed_y']
    dataframe['conf win pct'] = dataframe['conf_win_pct_x'] - dataframe['conf_win_pct_y']
    dataframe['Seed'] = dataframe['Seed_x'] - dataframe['Seed_y']
    dataframe['WSoS'] = dataframe['WSoS_x'] - dataframe['WSoS_y']
    dataframe['LSos'] = dataframe['LSoS_x'] - dataframe['LSoS_y']
    #left off here 1/1
    dataframe['Clutch Ft Pct'] = dataframe['clutch_FtPct_x'] - dataframe['clutch_FtPct_y']
    final_dataframe = dataframe[['Average Pts','Average PA','Home Record','Away Record','Coach Record','offensive rebounds','offensive rbs allowed',
                                 'avg_to_made','avg_to_allowed','conf win pct','Seed','WSoS','LSos','Clutch Ft Pct','Output']]
    return final_dataframe

def FY2019_data():
    FY2019_df = datapull(2019)
    
    
    #Record
    sql_wins19 = '''
    SELECT "WTeamID",COUNT("WTeamID") FROM "RegularSeasonDetailedResults" 
    WHERE "Season" = 2019
    GROUP BY "WTeamID"
    '''
    FY19_wins = pd.read_sql(sql_wins19, conn)
    FY19_wins["TeamID"] = FY19_wins["WTeamID"]
    FY19_wins["Wins"] = FY19_wins["count"]
    
    sql_losses19 = '''
    SELECT "LTeamID", COUNT("LTeamID") FROM "RegularSeasonDetailedResults"
    WHERE "Season" = 2018
    GROUP BY "LTeamID"
    '''
    
    FY19_losses = pd.read_sql(sql_losses18,conn)
    FY19_losses["TeamID"] = FY19_losses["LTeamID"]
    FY19_losses["Losses"] = FY19_losses["count"]
    
    FY19_record = pd.merge(FY19_wins, FY19_losses, on = "TeamID")
    FY19_record = FY19_record[["TeamID","Wins","Losses"]]
    FY19_record["win_pct"] = FY19_record["Wins"]/ (FY19_record["Losses"] + FY19_record["Wins"])
    FY19_record.to_sql("2019 Record", engine)
    
    #Strength of Schedule
    sql1128 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2019 Record"."TeamID","2019 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2019 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2019 Record"."TeamID"
WHERE "Season" = 2019) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 
    FY2019_sos_w = pd.read_sql(sql1128, conn)
    FY2019_sos_w.columns = ['TeamID','WSoS']
    FY2019_df_2 = pd.merge(FY2019_df, FY2019_sos_w, how = 'left',on = 'TeamID')
    
    sql1129 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2019 Record"."TeamID","2019 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2019 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2019 Record"."TeamID"
WHERE "Season" = 2019) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
'''

    FY2019_sos_l = pd.read_sql(sql1129, conn)
    FY2019_sos_l.columns = ['TeamID','LSoS']
    FY2019_df_3 = pd.merge(FY2019_df_2, FY2019_sos_l, how = 'left',on = 'TeamID')
    
    #FT%
    sql2011 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents19"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

    FY2019_clutch_ft_made = pd.read_sql(sql2011, conn)

    sql2012 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents19"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
    FY2019_clutch_ft_miss = pd.read_sql(sql2012, conn)

    FY2019_clutch_ft = pd.merge(FY2019_clutch_ft_made,FY2019_clutch_ft_miss, on = "WTeamID")
    FY2019_clutch_ft["clutch_FtPct"] = FY2019_clutch_ft['count_x']/(FY2019_clutch_ft['count_x']+ FY2019_clutch_ft['count_y'])
    FY2019_clutch_ft['TeamID'] = FY2019_clutch_ft['WTeamID']
    FY2019_clutch_ft = FY2019_clutch_ft[['TeamID','clutch_FtPct']]
    FY2019_df_4 = pd.merge(FY2019_df_3, FY2019_clutch_ft, how = 'left',on = 'TeamID')
    
    return FY2019_df_4
def set_up_MarchMadness():
    #classifier = ensemble()
    #classifier = test_model()
    model_df = FY2019_data()
    sql_tourney = r'''
    SELECT * FROM "March Madness 19 Tourney"
    '''
    tourney = pd.read_sql(sql_tourney, conn)
    round1 = tourney.head(35)
    bracket = pd.merge(model_df, round1, left_on = 'TeamID', right_on = 'Team1ID' )
    bracket_3 = pd.merge(bracket, model_df, left_on = 'Team2ID', right_on = 'TeamID')
    bracket_2 = bracket_3
    bracket_2['Seed_x'] = bracket_2['Team1Seed']
    bracket_2['Seed_y'] = bracket_2['Team2Seed']
    
    bracket_2['Average Pts'] = bracket_2['Average Pts_x'] - bracket_2['Average Pts_y']
    bracket_2['Average PA'] = bracket_2['Average PA_x'] - bracket_2['Average PA_y']
    bracket_2['Home Record'] = bracket_2['Home Record_x'] - bracket_2['Home Record_y']
    bracket_2['Away Record'] = bracket_2['Away Record_x'] - bracket_2['Away Record_y']
    bracket_2['Coach Record'] = bracket_2['coach_win_pct_x'] - bracket_2['coach_win_pct_y']
    bracket_2['offensive rebounds'] = bracket_2['orpg_x'] - bracket_2['orpg_y']
    bracket_2['offensive rbs allowed'] = bracket_2['orapg_x'] - bracket_2['orapg_y']
    bracket_2['avg_to_made'] = bracket_2['avg_to_made_x'] - bracket_2['avg_to_made_y']
    bracket_2['avg_to_allowed'] = bracket_2['avg_to_allowed_x'] - bracket_2['avg_to_allowed_y']
    bracket_2['conf win pct'] = bracket_2['conf_win_pct_x'] - bracket_2['conf_win_pct_y']
    bracket_2['Seed'] = bracket_2['Seed_x'] - bracket_2['Seed_y']
    bracket_2['WSoS'] = bracket_2['WSoS_x'] - bracket_2['WSoS_y']
    bracket_2['LSos'] = bracket_2['LSoS_x'] - bracket_2['LSoS_y']
    #left off here 1/1
    bracket_2['Clutch Ft Pct'] = bracket_2['clutch_FtPct_x'] - bracket_2['clutch_FtPct_y']
    March_Madness = bracket_2[['Average Pts','Average PA','Home Record','Away Record','Coach Record','offensive rebounds','offensive rbs allowed',
                                 'avg_to_made','avg_to_allowed','conf win pct','Seed','WSoS','LSos','Clutch Ft Pct']]
    return March_Madness,bracket_3,model_df

# =============================================================================
# def log_regression(X_train, X_test, y_train, y_test):
#  
#     import pickle
#     
# 
#     from sklearn.preprocessing import StandardScaler
#     sc_X = StandardScaler()
#     X_train = sc_X.fit_transform(X_train)
#     X_test = sc_X.transform(X_test)
#     
#     from sklearn.decomposition import PCA
#     pca = PCA(n_components = 6)
#     X_train = pca.fit_transform(X_train)
#     X_test = pca.transform(X_test)
#     filename2 = 'PCA_model'
#     pickle.dump(pca, open(filename2, 'wb'))
#    
#     explained_variance2 = pca.explained_variance_ratio_
# 
# #Fitting Logistic Regression to the Training Set
#     from sklearn.linear_model import LogisticRegression
# 
#     classifier = LogisticRegression()
#     classifier.fit(X_train, y_train)
# 
# #predicting the test set results
#     y_pred = classifier.predict(X_test)
# 
# #Make confusion matrix
#     from sklearn.metrics import classification_report, confusion_matrix
#     log_reg_pred = pd.DataFrame(data = y_pred, columns = ["Log_reg"])
#     import sklearn
#     accuracy = sklearn.metrics.accuracy_score(y_test, y_pred, normalize = True, sample_weight = None)
#     d = {'log_reg': accuracy}
#     log_reg_acc = pd.DataFrame(data = d, index = [0])
#     
#     filename = 'finalized_log_reg.sav'
#     pickle.dump(classifier, open(filename, 'wb'))
#     
#     
#     
#     
#     
#     
#     return log_reg_pred, log_reg_acc
# 
# def KNN(X_train, X_test, y_train, y_test):
#   
# 
#     from sklearn.preprocessing import StandardScaler
#     sc_X = StandardScaler()
#     X_train = sc_X.fit_transform(X_train)
#     X_test = sc_X.transform(X_test)
#     
#     from sklearn.decomposition import PCA
#     pca = PCA(n_components = 6)
#     X_train = pca.fit_transform(X_train)
#     X_test = pca.transform(X_test)
#     
# 
# #Fitting Logistic Regression to the Training Set
#     from sklearn.neighbors import KNeighborsClassifier
# 
#     classifier = KNeighborsClassifier()
#     classifier.fit(X_train, y_train)
# 
# #predicting the test set results
#     y_pred = classifier.predict(X_test)
#     
#     from sklearn.metrics import confusion_matrix
#     cm = confusion_matrix(y_test, y_pred)
#     from sklearn.metrics import classification_report, confusion_matrix
#     knn_pred = pd.DataFrame(data = y_pred, columns = ["KNN"])
#     import sklearn
#     accuracy = sklearn.metrics.accuracy_score(y_test, y_pred, normalize = True, sample_weight = None)
#     d = {'KNN': accuracy}
#     knn_acc = pd.DataFrame(data = d, index = [0])
#     
#     filename = 'KNN_model.sav'
#     pickle.dump(classifier, open(filename, 'wb'))
#     
#     
#     return knn_pred, knn_acc
# 
# 
# def svm(X_train, X_test, y_train, y_test):
#     #tune hyperparamters
# 
#     from sklearn.preprocessing import StandardScaler
#     sc_X = StandardScaler()
#     X_train = sc_X.fit_transform(X_train)
#     X_test = sc_X.transform(X_test)
# 
# #Fitting Logistic Regression to the Training Set
#     from sklearn.svm import SVC
# 
#     classifier = SVC()
#     classifier.fit(X_train, y_train)
# 
# #predicting the test set results
#     y_pred = classifier.predict(X_test)
# 
# #Make confusion matrix
#     from sklearn.metrics import confusion_matrix
#     cm = confusion_matrix(y_test, y_pred)
#     from sklearn.metrics import classification_report, confusion_matrix
#     svm_pred = pd.DataFrame(data = y_pred, columns = ["SVM"])
#     import sklearn
#     accuracy = sklearn.metrics.accuracy_score(y_test, y_pred, normalize = True, sample_weight = None)
#     d = {'SVM': accuracy}
#     svm_acc = pd.DataFrame(data = d, index = [0])
#  
#     
#     filename = 'SVM_Model.sav'
#     pickle.dump(classifier, open(filename, 'wb'))
#     return svm_pred, svm_acc
# 
# def random_forest(X_train, X_test, y_train, y_test):
# 
# 
#     
# #Fitting Logistic Regression to the Training Set
#     from sklearn.ensemble import RandomForestClassifier
# 
#     classifier = RandomForestClassifier()
#     
#     classifier.fit(X_train, y_train)
# 
# #predicting the test set results
#     y_pred = classifier.predict(X_test)
#     from sklearn.metrics import classification_report, confusion_matrix
#     rf_pred = pd.DataFrame(data = y_pred, columns = ["Random_Forest"])
#     import sklearn
#     accuracy = sklearn.metrics.accuracy_score(y_test, y_pred, normalize = True, sample_weight = None)
#     d = {'RF': accuracy}
#     rf_acc = pd.DataFrame(data = d, index = [0])
#     
#     filename = 'RF_model.sav'
#     pickle.dump(classifier, open(filename, 'wb'))
#     
#     return rf_pred, rf_acc
#     
# 
# 
# def ensemble():
#     from sklearn.model_selection import KFold
#     kf = KFold(n_splits = 10, shuffle = False)
#     dataset = set_up_model()
#     X = dataset[['Average Pts','Average PA','Home Record','Away Record','Coach Record','offensive rebounds','offensive rbs allowed',
#                                  'avg_to_made','avg_to_allowed','conf win pct','Seed','WSoS','LSos','Clutch Ft Pct']]
#     y = dataset[['Output']]
#     
#     conc_df = pd.DataFrame()
#     df20 = pd.DataFrame()
#     conc_acc = pd.DataFrame()
#     
#     
#     X_updated = X.as_matrix()
#     y_updated = y.as_matrix()
#     
#     
#     
#     kf = KFold(n_splits = 10, shuffle = False)
#     for train_index, test_index in kf.split(X):
#         X_train, X_test = X_updated[train_index], X_updated[test_index]
#         y_train, y_test = y_updated[train_index], y_updated[test_index]
#         df4 = pd.DataFrame(y_test, columns = ['Actual'])
#         log_reg_pred, log_reg_acc = log_regression(X_train, X_test, y_train, y_test)
#         svm_pred, svm_acc = svm(X_train, X_test, y_train, y_test)
#         RF_pred, RF_acc = random_forest(X_train, X_test, y_train, y_test)
#         knn_pred, knn_acc = KNN(X_train, X_test, y_train, y_test)
#         df5 = df4.join(svm_pred, how = "outer", lsuffix = "_", rsuffix = "_")
#         df6 = df5.join(RF_pred, how = "outer", lsuffix = "_", rsuffix = "_")
#         df7 = df6.join(knn_pred, how = "outer", lsuffix = "_", rsuffix = "_")
#         df8 = df7.join(log_reg_pred, how = "outer", lsuffix = "_", rsuffix = "_")
#         df21 = df20.join(svm_acc, how = "outer", lsuffix = "_", rsuffix = "_")
#         df22 = df21.join(RF_acc, how = "outer", lsuffix = "_", rsuffix = "_")
#         df23 = df22.join(knn_acc, how = "outer", lsuffix = "_", rsuffix = "_")
#         df24 = df23.join(log_reg_acc,how = "outer", lsuffix = "_", rsuffix = "_")
#     
#         conc_df = conc_df.append(df8, ignore_index = True)
#         conc_acc = conc_acc.append(df24, ignore_index = True)
#         import statistics
#         conc_acc['SVM_avg'] = statistics.mean(conc_acc['SVM'])
#         conc_acc['RF_avg'] = statistics.mean(conc_acc['RF'])
#         conc_acc['KNN_avg'] = statistics.mean(conc_acc['KNN'])
#         conc_acc['log_reg_avg'] = statistics.mean(conc_acc['log_reg'])
#         
#         
#         
#         #build ensemble
#         #do this with K_Fold
#         #figure out how to get original dataset ie which one they erred on
#         X_2 = conc_df[['SVM', 'Random_Forest', 'KNN', 'Log_reg']]
#         y_2 = conc_df[['Actual']]
#         
#         X_2 = X_2.values
#         y_2 = y_2.values
#         final_error_anal = pd.DataFrame()
#         final_error_anal2 = pd.DataFrame()
#         acc3 = pd.DataFrame()
#         
#         from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier
#         
#         clf_meta = GradientBoostingClassifier()
#         for train_index, test_index in kf.split(X_2):
#             X_train2, X_test2 = X_2[train_index], X_2[test_index]
#             y_train2, y_test2 = y_2[train_index], y_2[test_index]
#             clf_meta.fit(X_train2, y_train2)
#             meta_predictions = clf_meta.predict(X_test2)
#             filename3 = 'meta_model.sav'
#             pickle.dump(clf_meta, open(filename3, 'wb'))
#             error_df3 = pd.DataFrame(meta_predictions, columns = ['Predicted'])
#             print(error_df3)
#             error_df2 = pd.DataFrame(y_test2, columns = ['Actual'])
#             final_error_anal = error_df2.join(error_df3, how = "outer", lsuffix = "_", rsuffix = "_")
#             final_error_anal2 = final_error_anal2.append(final_error_anal)
#             import sklearn
#             acc2 = sklearn.metrics.accuracy_score(y_test2, meta_predictions)
#             acc = {'Accuracy': acc2}
#             stack_acc = pd.DataFrame(data = acc, index = [0])
#             acc3 = acc3.append(stack_acc)
#             print(acc3)
#         
# =============================================================================
def main(model, bracket2):   
    #Get seeds
    #model_df = FY2018_data()
    
    
    #first round
     
        
    PCA_class = pickle.load(open('PCA_model', 'rb'))
    PCA_pred = PCA_class.transform(model)  
      
  
            
    log_reg_class = pickle.load(open('finalized_log_reg.sav', 'rb'))
    log_reg_pred = log_reg_class.predict(PCA_pred)  
    
    KNN_class = pickle.load(open('KNN_model.sav', 'rb'))
    KNN_pred = KNN_class.predict(PCA_pred) 
    
    SVM_class = pickle.load(open('SVM_Model.sav', 'rb'))
    SVM_pred = SVM_class.predict(model)
    
    RF_class = pickle.load(open('RF_model.sav', 'rb'))
    RF_pred = RF_class.predict(model)
    
    NB_class = pickle.load(open('NB_model.sav','rb'))
    NB_pred = NB_class.predict(model)
    
    log_reg_df = pd.DataFrame(log_reg_pred, columns = ['Logistic'])
    KNN_df = pd.DataFrame(KNN_pred, columns = ["KNN"])
    SVM_df = pd.DataFrame(SVM_pred, columns = ['SVM'])
    RF_df = pd.DataFrame(RF_pred, columns = ['RF'])
    NB_df = pd.DataFrame(NB_pred, columns = ['NB'])
    
    dfs = [log_reg_df, KNN_df, SVM_df, RF_df, NB_df]
    bracket_preds = dfs[0].join(dfs[1:])
    
    #RF_class = pickle.load(open('RF_model.sav', 'rb'))
    #RF_pred = RF_class.predict(model)
    
    bracket_preds = bracket_preds.values
    clf_meta = pickle.load(open('meta_model.sav', 'rb'))
    FINAL_prediction = clf_meta.predict(bracket_preds)
    FINAL_prob = clf_meta.predict_proba(bracket_preds)[:,0]
    final_prob = pd.DataFrame(FINAL_prob,columns = ['Final Probability'])
    absolute_final_df = pd.DataFrame(FINAL_prediction, columns = ['Final Prediction'])
    
    absolute_final_df = absolute_final_df.join(bracket2)
    absolute_final_df = absolute_final_df.join(final_prob)

    absolute_final_df = absolute_final_df[['Final Prediction','Team1ID','Team2ID','Slot','Final Probability']]
    round1_lg, width = absolute_final_df.shape
    
    #do this once it works
    return absolute_final_df, bracket2

def round_2():  
    final_bracket,bracket2,model_df = set_up_MarchMadness() 
    absolute_final_df,bracket_3 = main(final_bracket,bracket2)
    #2nd round
    round_2 = r'''
    SELECT * FROM "March Madness 19 Tourney"
    '''
    round2 = pd.read_sql(round_2, conn)
    round2_df=pd.DataFrame()
    round2_df = round2.iloc[35:51,:]
    round2_df2 = pd.DataFrame()
    absolute_final_df['Winner'] = 1
    
    
    for i in range(0,35):
        if absolute_final_df.iloc[i,0] == 1:
            absolute_final_df.iloc[i,5] = absolute_final_df.iloc[i,1]
            
        
        if absolute_final_df.iloc[i,0] == 0:
            absolute_final_df.iloc[i,5] = absolute_final_df.iloc[i,2]
    
    
            
    round2_df2 = pd.merge(absolute_final_df,round2_df, left_on = 'Slot', right_on = 'StrongSeed')
    round2_df3 = pd.merge(round2_df,absolute_final_df, left_on = 'WeakSeed', right_on = 'Slot') 
    round2_df4 = pd.merge(round2_df2, round2_df3, on = 'StrongSeed')     
    round2_df4['Team1ID'] = round2_df4['Winner_x']
    round2_df4['Team2ID'] = round2_df4['Winner_y']
    round2_df20 = round2_df4[['Slot_x_x','Slot_x_y','Team1ID','Team2ID','Final Probability_x','Final Probability_y']]
    round2_df4 = round2_df4[['Slot_x_x','Slot_x_y','Team1ID','Team2ID']]
    
    
    #3/2 - copy/paste this for later rounds!
    
    round2_df20['Round'] = 2
    round2_df20 = pd.merge(round2_df20,df_teams, left_on = 'Team1ID', right_on = 'TeamID')
    round2_df20 = pd.merge(round2_df20, df_teams, left_on = 'Team2ID', right_on = 'TeamID')
    round2_df20['TeamName1'] = round2_df20['TeamName_x']
    round2_df20['TeamName2'] = round2_df20['TeamName_y'] 
    round2_df20 = round2_df20[['Team1ID','Team2ID','Round','TeamName1','TeamName2','Final Probability_x','Final Probability_y']]
    
    round2_df5 = pd.merge(model_df, round2_df4, left_on  = 'TeamID',right_on = 'Team1ID')
    round2_df7 = pd.merge(round2_df5, model_df, left_on = 'Team2ID', right_on = 'TeamID')
    #this has most data for 2 teams
    
    #get seed
    round2_df8 = get_seed[['Team1ID','Team1Seed']]
    round2_df9 = get_seed[['Team2ID','Team2Seed']]
    round2_df9['Team1ID'] = round2_df9['Team2ID']
    round2_df9['Team1Seed'] = round2_df9['Team2Seed']
    round2_df9 = round2_df9[['Team1ID','Team1Seed']]
    round2_df10 = round2_df8.append(round2_df9, sort = True, ignore_index = True)
    round2_df10['TeamSeedID'] = round2_df10['Team1ID']
    round2_df10 = round2_df10[['TeamSeedID','Team1Seed']]
    round2_df10 = round2_df10.drop_duplicates(subset='TeamSeedID', keep="last")
    
    #combine with seed
    round2_df11 = pd.merge(round2_df10, round2_df7, left_on = 'TeamSeedID', right_on = 'Team1ID')
    round2_df13 = pd.merge(round2_df11, round2_df10, left_on = 'Team2ID', right_on = 'TeamSeedID')
    round2_df13['Slot'] = round2_df13['Slot_x_y']
    
    #use this for slot
    round2_df12 = round2_df13[['Team1Seed_x', 'Average Pts_x',
       'Average PA_x', 'Home Record_x', 'Away Record_x', 'coach_win_pct_x',
       'orpg_x', 'orapg_x', 'avg_to_made_x', 'avg_to_allowed_x',
       'conf_win_pct_x', 'WSoS_x', 'LSoS_x', 'clutch_FtPct_x',
       'Slot_x_x', 'Slot', 'Team1ID', 'Team2ID',
       'Average Pts_y', 'Average PA_y', 'Home Record_y', 'Away Record_y',
       'coach_win_pct_y', 'orpg_y', 'orapg_y', 'avg_to_made_y',
       'avg_to_allowed_y', 'conf_win_pct_y', 'Seed_y', 'WSoS_y', 'LSoS_y',
       'clutch_FtPct_y', 'Team1Seed_y']]
    
    round2_df12['Seed_x'] = round2_df12['Team1Seed_x']
    round2_df12['Seed_y'] = round2_df12['Team1Seed_y']
    
    round2_df12['Average Pts'] = round2_df12['Average Pts_x'] - round2_df12['Average Pts_y']
    round2_df12['Average PA'] = round2_df12['Average PA_x'] - round2_df12['Average PA_y']
    round2_df12['Home Record'] = round2_df12['Home Record_x'] - round2_df12['Home Record_y']
    round2_df12['Away Record'] = round2_df12['Away Record_x'] - round2_df12['Away Record_y']
    round2_df12['Coach Record'] = round2_df12['coach_win_pct_x'] - round2_df12['coach_win_pct_y']
    round2_df12['offensive rebounds'] = round2_df12['orpg_x'] - round2_df12['orpg_y']
    round2_df12['offensive rbs allowed'] = round2_df12['orapg_x'] - round2_df12['orapg_y']
    round2_df12['avg_to_made'] = round2_df12['avg_to_made_x'] - round2_df12['avg_to_made_y']
    round2_df12['avg_to_allowed'] = round2_df12['avg_to_allowed_x'] - round2_df12['avg_to_allowed_y']
    round2_df12['conf win pct'] = round2_df12['conf_win_pct_x'] - round2_df12['conf_win_pct_y']
    round2_df12['Seed'] = round2_df12['Seed_x'] - round2_df12['Seed_y']
    round2_df12['WSoS'] = round2_df12['WSoS_x'] - round2_df12['WSoS_y']
    round2_df12['LSos'] = round2_df12['LSoS_x'] - round2_df12['LSoS_y']
    #left off here 1/1
    round2_df12['Clutch Ft Pct'] = round2_df12['clutch_FtPct_x'] - round2_df12['clutch_FtPct_y']
    final_bracket_round2 = round2_df12[['Average Pts','Average PA','Home Record','Away Record','Coach Record','offensive rebounds','offensive rbs allowed',
                                 'avg_to_made','avg_to_allowed','conf win pct','Seed','WSoS','LSos','Clutch Ft Pct']]
    
    #run through model
    return final_bracket_round2, round2_df13,round2_df20
            

def round_3():
    #figure out the bracket 2 (make sure every round has one which is used to join)
    a,b, model_df = set_up_MarchMadness()
    final_bracket_round2, bracket2,round2_df20 = round_2()
    round_2_results, bracket_3 = main(final_bracket_round2, bracket2)
    
    round_3 = r'''
    SELECT * FROM "March Madness 19 Tourney"
    '''
    round3 = pd.read_sql(round_3, conn)
    round3_df=pd.DataFrame()
    round3_df = round3.iloc[51:59,:]
    round3_df2 = pd.DataFrame()
    
    round_2_results['Winner'] = 1
    
    
    for i in range(0,16):
        if round_2_results.iloc[i,0] == 1:
            round_2_results.iloc[i,5] = round_2_results.iloc[i,1]
            
        
        if round_2_results.iloc[i,0] == 0:
            round_2_results.iloc[i,5] = round_2_results.iloc[i,2]
    
    
           
    round3_df2 = pd.merge(round_2_results,round3_df, left_on = 'Slot', right_on = 'StrongSeed')
    round3_df3 = pd.merge(round3_df,round_2_results, left_on = 'WeakSeed', right_on = 'Slot') 
    round3_df4 = pd.merge(round3_df2, round3_df3, on = 'StrongSeed')    
    round3_df20 = round3_df4[['Slot_x_x','Slot_x_y','Winner_x','Winner_y','Final Probability_x','Final Probability_y']]
    round3_df4 = round3_df4[['Slot_x_y','Winner_x','Winner_y']]
    
   
    round3_df20['Round'] = 3
    round3_df20['Team1ID'] = round3_df20['Winner_x']
    round3_df20['Team2ID'] = round3_df20['Winner_y']
    round3_df20 = pd.merge(round3_df20,df_teams, left_on = 'Team1ID', right_on = 'TeamID')
    round3_df20 = pd.merge(round3_df20, df_teams, left_on = 'Team2ID', right_on = 'TeamID')
    round3_df20['TeamName1'] = round3_df20['TeamName_x']
    round3_df20['TeamName2'] = round3_df20['TeamName_y'] 
    round3_df20 = round3_df20[['Team1ID','Team2ID','Round','TeamName1','TeamName2','Final Probability_x','Final Probability_y']]
    round3_df20 = round3_df20.append(round2_df20)
    
    round3_df4['Team1ID'] = round3_df4['Winner_x']
    round3_df4['Team2ID'] = round3_df4['Winner_y']
    round3_df4 = round3_df4[['Slot_x_y','Team1ID','Team2ID']]
    round3_df5 = pd.merge(model_df, round3_df4, left_on  = 'TeamID',right_on = 'Team1ID')
    round3_df7 = pd.merge(round3_df5, model_df, left_on = 'Team2ID', right_on = 'TeamID')
    #get seed
    round3_df8 = get_seed[['Team1ID','Team1Seed']]
    round3_df9 = get_seed[['Team2ID','Team2Seed']]
    round3_df9['Team1ID'] = round3_df9['Team2ID']
    round3_df9['Team1Seed'] = round3_df9['Team2Seed']
    round3_df9 = round3_df9[['Team1ID','Team1Seed']]
    round3_df10 = round3_df8.append(round3_df9, sort = True, ignore_index = True)
    round3_df10['TeamSeedID'] = round3_df10['Team1ID']
    round3_df10 = round3_df10[['TeamSeedID','Team1Seed']]
    round3_df10 = round3_df10.drop_duplicates(subset='TeamSeedID', keep="last")
    
    #combine with seed
    round3_df11 = pd.merge(round3_df10, round3_df7, left_on = 'TeamSeedID', right_on = 'Team1ID')
    round3_df13 = pd.merge(round3_df11, round3_df10, left_on = 'Team2ID', right_on = 'TeamSeedID')
    round3_df13['Slot'] = round3_df13['Slot_x_y']
    
    #use this for slot
    round3_df12 = round3_df13[['Team1Seed_x', 'Average Pts_x',
       'Average PA_x', 'Home Record_x', 'Away Record_x', 'coach_win_pct_x',
       'orpg_x', 'orapg_x', 'avg_to_made_x', 'avg_to_allowed_x',
       'conf_win_pct_x', 'WSoS_x', 'LSoS_x', 'clutch_FtPct_x',
       'Slot', 'Team1ID', 'Team2ID',
       'Average Pts_y', 'Average PA_y', 'Home Record_y', 'Away Record_y',
       'coach_win_pct_y', 'orpg_y', 'orapg_y', 'avg_to_made_y',
       'avg_to_allowed_y', 'conf_win_pct_y', 'Seed_y', 'WSoS_y', 'LSoS_y',
       'clutch_FtPct_y', 'Team1Seed_y']]
    
    round3_df12['Seed_x'] = round3_df12['Team1Seed_x']
    round3_df12['Seed_y'] = round3_df12['Team1Seed_y']
    
    round3_df12['Average Pts'] = round3_df12['Average Pts_x'] - round3_df12['Average Pts_y']
    round3_df12['Average PA'] = round3_df12['Average PA_x'] - round3_df12['Average PA_y']
    round3_df12['Home Record'] = round3_df12['Home Record_x'] - round3_df12['Home Record_y']
    round3_df12['Away Record'] = round3_df12['Away Record_x'] - round3_df12['Away Record_y']
    round3_df12['Coach Record'] = round3_df12['coach_win_pct_x'] - round3_df12['coach_win_pct_y']
    round3_df12['offensive rebounds'] = round3_df12['orpg_x'] - round3_df12['orpg_y']
    round3_df12['offensive rbs allowed'] = round3_df12['orapg_x'] - round3_df12['orapg_y']
    round3_df12['avg_to_made'] = round3_df12['avg_to_made_x'] - round3_df12['avg_to_made_y']
    round3_df12['avg_to_allowed'] = round3_df12['avg_to_allowed_x'] - round3_df12['avg_to_allowed_y']
    round3_df12['conf win pct'] = round3_df12['conf_win_pct_x'] - round3_df12['conf_win_pct_y']
    round3_df12['Seed'] = round3_df12['Seed_x'] - round3_df12['Seed_y']
    round3_df12['WSoS'] = round3_df12['WSoS_x'] - round3_df12['WSoS_y']
    round3_df12['LSos'] = round3_df12['LSoS_x'] - round3_df12['LSoS_y']
    #left off here 1/1
    round3_df12['Clutch Ft Pct'] = round3_df12['clutch_FtPct_x'] - round3_df12['clutch_FtPct_y']
    final_bracket_round3 = round3_df12[['Average Pts','Average PA','Home Record','Away Record','Coach Record','offensive rebounds','offensive rbs allowed',
                                 'avg_to_made','avg_to_allowed','conf win pct','Seed','WSoS','LSos','Clutch Ft Pct']]
    
    #run through model
    return final_bracket_round3, round3_df13, round3_df20

def round_4():
    #figure out the bracket 2 (make sure every round has one which is used to join)
    final_bracket_round3, bracket3,round3_df20 = round_3()
    round_3_results, bracket_4 = main(final_bracket_round3, bracket3)
    a,b, model_df = set_up_MarchMadness()
    round_4 = r'''
    SELECT * FROM "March Madness 19 Tourney"
    '''
    round4 = pd.read_sql(round_4, conn)
    round4_df=pd.DataFrame()
    round4_df = round4.iloc[59:63,:]
    round4_df2 = pd.DataFrame()
    
    round_3_results['Winner'] = 1
    
    
    for i in range(0,8):
        if round_3_results.iloc[i,0] == 1:
            round_3_results.iloc[i,5] = round_3_results.iloc[i,1]
            
        
        if round_3_results.iloc[i,0] == 0:
            round_3_results.iloc[i,5] = round_3_results.iloc[i,2]
    
    
           
    round4_df2 = pd.merge(round_3_results,round4_df, left_on = 'Slot', right_on = 'StrongSeed')
    round4_df3 = pd.merge(round4_df,round_3_results, left_on = 'WeakSeed', right_on = 'Slot') 
    round4_df4 = pd.merge(round4_df2, round4_df3, on = 'StrongSeed')    
    
    
    
    round4_df4['Team1ID'] = round4_df4['Winner_x']
    round4_df4['Team2ID'] = round4_df4['Winner_y']
    round4_df20 = round4_df4[['Slot_x_x','Slot_x_y','Team1ID','Team2ID','Final Probability_x','Final Probability_y']]
    round4_df4 = round4_df4[['Slot_x_y','Team1ID','Team2ID']]
    
    
    round4_df20['Round'] = 4
    round4_df20 = pd.merge(round4_df20,df_teams, left_on = 'Team1ID', right_on = 'TeamID')
    round4_df20 = pd.merge(round4_df20, df_teams, left_on = 'Team2ID', right_on = 'TeamID')
    round4_df20['TeamName1'] = round4_df20['TeamName_x']
    round4_df20['TeamName2'] = round4_df20['TeamName_y'] 
    round4_df20 = round4_df20[['Team1ID','Team2ID','Round','TeamName1','TeamName2','Final Probability_x','Final Probability_y']]
    round4_df20 = round4_df20.append(round3_df20)
    
    
    round4_df5 = pd.merge(model_df, round4_df4, left_on  = 'TeamID',right_on = 'Team1ID')
    round4_df7 = pd.merge(round4_df5, model_df, left_on = 'Team2ID', right_on = 'TeamID')
    #get seed
    round4_df8 = get_seed[['Team1ID','Team1Seed']]
    round4_df9 = get_seed[['Team2ID','Team2Seed']]
    round4_df9['Team1ID'] = round4_df9['Team2ID']
    round4_df9['Team1Seed'] = round4_df9['Team2Seed']
    round4_df9 = round4_df9[['Team1ID','Team1Seed']]
    round4_df10 = round4_df8.append(round4_df9, sort = True, ignore_index = True)
    round4_df10['TeamSeedID'] = round4_df10['Team1ID']
    round4_df10 = round4_df10[['TeamSeedID','Team1Seed']]
    round4_df10 = round4_df10.drop_duplicates(subset='TeamSeedID', keep="last")
    
    #combine with seed
    round4_df11 = pd.merge(round4_df10, round4_df7, left_on = 'TeamSeedID', right_on = 'Team1ID')
    round4_df13 = pd.merge(round4_df11, round4_df10, left_on = 'Team2ID', right_on = 'TeamSeedID')
    round4_df13['Slot'] = round4_df13['Slot_x_y']
    
    #use this for slot
    round4_df12 = round4_df13[['Team1Seed_x', 'Average Pts_x',
       'Average PA_x', 'Home Record_x', 'Away Record_x', 'coach_win_pct_x',
       'orpg_x', 'orapg_x', 'avg_to_made_x', 'avg_to_allowed_x',
       'conf_win_pct_x', 'WSoS_x', 'LSoS_x', 'clutch_FtPct_x',
       'Slot', 'Team1ID', 'Team2ID',
       'Average Pts_y', 'Average PA_y', 'Home Record_y', 'Away Record_y',
       'coach_win_pct_y', 'orpg_y', 'orapg_y', 'avg_to_made_y',
       'avg_to_allowed_y', 'conf_win_pct_y', 'Seed_y', 'WSoS_y', 'LSoS_y',
       'clutch_FtPct_y', 'Team1Seed_y']]
    
    round4_df12['Seed_x'] = round4_df12['Team1Seed_x']
    round4_df12['Seed_y'] = round4_df12['Team1Seed_y']
    
    round4_df12['Average Pts'] = round4_df12['Average Pts_x'] - round4_df12['Average Pts_y']
    round4_df12['Average PA'] = round4_df12['Average PA_x'] - round4_df12['Average PA_y']
    round4_df12['Home Record'] = round4_df12['Home Record_x'] - round4_df12['Home Record_y']
    round4_df12['Away Record'] = round4_df12['Away Record_x'] - round4_df12['Away Record_y']
    round4_df12['Coach Record'] = round4_df12['coach_win_pct_x'] - round4_df12['coach_win_pct_y']
    round4_df12['offensive rebounds'] = round4_df12['orpg_x'] - round4_df12['orpg_y']
    round4_df12['offensive rbs allowed'] = round4_df12['orapg_x'] - round4_df12['orapg_y']
    round4_df12['avg_to_made'] = round4_df12['avg_to_made_x'] - round4_df12['avg_to_made_y']
    round4_df12['avg_to_allowed'] = round4_df12['avg_to_allowed_x'] - round4_df12['avg_to_allowed_y']
    round4_df12['conf win pct'] = round4_df12['conf_win_pct_x'] - round4_df12['conf_win_pct_y']
    round4_df12['Seed'] = round4_df12['Seed_x'] - round4_df12['Seed_y']
    round4_df12['WSoS'] = round4_df12['WSoS_x'] - round4_df12['WSoS_y']
    round4_df12['LSos'] = round4_df12['LSoS_x'] - round4_df12['LSoS_y']
    #left off here 1/1
    round4_df12['Clutch Ft Pct'] = round4_df12['clutch_FtPct_x'] - round4_df12['clutch_FtPct_y']
    final_bracket_round4 = round4_df12[['Average Pts','Average PA','Home Record','Away Record','Coach Record','offensive rebounds','offensive rbs allowed',
                                 'avg_to_made','avg_to_allowed','conf win pct','Seed','WSoS','LSos','Clutch Ft Pct']]
    
    return final_bracket_round4, round4_df13,round4_df20

def round_5():
    #figure out the bracket 2 (make sure every round has one which is used to join)
    final_bracket_round4, bracket4,round4_df20 = round_4()
    round_4_results, bracket_5 = main(final_bracket_round4, bracket4)
    a,b, model_df = set_up_MarchMadness()
    
    round_5 = r'''
    SELECT * FROM "March Madness 19 Tourney"
    '''
    round5 = pd.read_sql(round_5, conn)
    round5_df=pd.DataFrame()
    round5_df = round5.iloc[63:65,:]
    round5_df2 = pd.DataFrame()
    
    round_4_results['Winner'] = 1
    
    
    for i in range(0,4):
        if round_4_results.iloc[i,0] == 1:
            round_4_results.iloc[i,5] = round_4_results.iloc[i,1]
            
        
        if round_4_results.iloc[i,0] == 0:
            round_4_results.iloc[i,5] = round_4_results.iloc[i,2]
    
    
           
    round5_df2 = pd.merge(round_4_results,round5_df, left_on = 'Slot', right_on = 'StrongSeed')
    round5_df3 = pd.merge(round5_df,round_4_results, left_on = 'WeakSeed', right_on = 'Slot') 
    round5_df4 = pd.merge(round5_df2, round5_df3, on = 'StrongSeed')    
    
    round5_df4['Team1ID'] = round5_df4['Winner_x']
    round5_df4['Team2ID'] = round5_df4['Winner_y']
    round5_df20 = round5_df4[['Slot_x_x','Slot_x_y','Team1ID','Team2ID','Final Probability_x','Final Probability_y']]
    round5_df4 = round5_df4[['Slot_x_y','Team1ID','Team2ID']]
    
    
    round5_df20['Round'] = 5
    round5_df20 = pd.merge(round5_df20,df_teams, left_on = 'Team1ID', right_on = 'TeamID')
    round5_df20 = pd.merge(round5_df20, df_teams, left_on = 'Team2ID', right_on = 'TeamID')
    round5_df20['TeamName1'] = round5_df20['TeamName_x']
    round5_df20['TeamName2'] = round5_df20['TeamName_y'] 
    round5_df20 = round5_df20[['Team1ID','Team2ID','Round','TeamName1','TeamName2','Final Probability_x','Final Probability_y']]
    round5_df20 = round5_df20.append(round4_df20)
    
    
    
    
    round5_df5 = pd.merge(model_df, round5_df4, left_on  = 'TeamID',right_on = 'Team1ID')
    round5_df7 = pd.merge(round5_df5, model_df, left_on = 'Team2ID', right_on = 'TeamID')
    #get seed
    round5_df8 = get_seed[['Team1ID','Team1Seed']]
    round5_df9 = get_seed[['Team2ID','Team2Seed']]
    round5_df9['Team1ID'] = round5_df9['Team2ID']
    round5_df9['Team1Seed'] = round5_df9['Team2Seed']
    round5_df9 = round5_df9[['Team1ID','Team1Seed']]
    round5_df10 = round5_df8.append(round5_df9, sort = True, ignore_index = True)
    round5_df10['TeamSeedID'] = round5_df10['Team1ID']
    round5_df10 = round5_df10[['TeamSeedID','Team1Seed']]
    round5_df10 = round5_df10.drop_duplicates(subset='TeamSeedID', keep="last")
    
    #combine with seed
    round5_df11 = pd.merge(round5_df10, round5_df7, left_on = 'TeamSeedID', right_on = 'Team1ID')
    round5_df13 = pd.merge(round5_df11, round5_df10, left_on = 'Team2ID', right_on = 'TeamSeedID')
    round5_df13['Slot'] = round5_df13['Slot_x_y']
    
    #use this for slot
    round5_df12 = round5_df13[['Team1Seed_x', 'Average Pts_x',
       'Average PA_x', 'Home Record_x', 'Away Record_x', 'coach_win_pct_x',
       'orpg_x', 'orapg_x', 'avg_to_made_x', 'avg_to_allowed_x',
       'conf_win_pct_x', 'WSoS_x', 'LSoS_x', 'clutch_FtPct_x',
       'Slot', 'Team1ID', 'Team2ID',
       'Average Pts_y', 'Average PA_y', 'Home Record_y', 'Away Record_y',
       'coach_win_pct_y', 'orpg_y', 'orapg_y', 'avg_to_made_y',
       'avg_to_allowed_y', 'conf_win_pct_y', 'Seed_y', 'WSoS_y', 'LSoS_y',
       'clutch_FtPct_y', 'Team1Seed_y']]
    
    round5_df12['Seed_x'] = round5_df12['Team1Seed_x']
    round5_df12['Seed_y'] = round5_df12['Team1Seed_y']
    
    round5_df12['Average Pts'] = round5_df12['Average Pts_x'] - round5_df12['Average Pts_y']
    round5_df12['Average PA'] = round5_df12['Average PA_x'] - round5_df12['Average PA_y']
    round5_df12['Home Record'] = round5_df12['Home Record_x'] - round5_df12['Home Record_y']
    round5_df12['Away Record'] = round5_df12['Away Record_x'] - round5_df12['Away Record_y']
    round5_df12['Coach Record'] = round5_df12['coach_win_pct_x'] - round5_df12['coach_win_pct_y']
    round5_df12['offensive rebounds'] = round5_df12['orpg_x'] - round5_df12['orpg_y']
    round5_df12['offensive rbs allowed'] = round5_df12['orapg_x'] - round5_df12['orapg_y']
    round5_df12['avg_to_made'] = round5_df12['avg_to_made_x'] - round5_df12['avg_to_made_y']
    round5_df12['avg_to_allowed'] = round5_df12['avg_to_allowed_x'] - round5_df12['avg_to_allowed_y']
    round5_df12['conf win pct'] = round5_df12['conf_win_pct_x'] - round5_df12['conf_win_pct_y']
    round5_df12['Seed'] = round5_df12['Seed_x'] - round5_df12['Seed_y']
    round5_df12['WSoS'] = round5_df12['WSoS_x'] - round5_df12['WSoS_y']
    round5_df12['LSos'] = round5_df12['LSoS_x'] - round5_df12['LSoS_y']
    #left off here 1/1
    round5_df12['Clutch Ft Pct'] = round5_df12['clutch_FtPct_x'] - round5_df12['clutch_FtPct_y']
    final_bracket_round5 = round5_df12[['Average Pts','Average PA','Home Record','Away Record','Coach Record','offensive rebounds','offensive rbs allowed',
                                 'avg_to_made','avg_to_allowed','conf win pct','Seed','WSoS','LSos','Clutch Ft Pct']]
    
    return final_bracket_round5, round5_df13,round5_df20

def round_6():
    final_bracket_round5, bracket5,round5_df20 = round_5()
    round_5_results, bracket_6 = main(final_bracket_round5, bracket5)
    a,b, model_df = set_up_MarchMadness()
    
    round_6 = r'''
    SELECT * FROM "March Madness 19 Tourney"
    '''
    round6 = pd.read_sql(round_6, conn)
    round6_df=pd.DataFrame()
    round6_df = round6.iloc[65:66,:]
    round6_df2 = pd.DataFrame()
    
    round_5_results['Winner'] = 1
    
    
    for i in range(0,2):
        if round_5_results.iloc[i,0] == 1:
            round_5_results.iloc[i,5] = round_5_results.iloc[i,1]
            
        
        if round_5_results.iloc[i,0] == 0:
            round_5_results.iloc[i,5] = round_5_results.iloc[i,2]
    
    
           
    round6_df2 = pd.merge(round_5_results,round6_df, left_on = 'Slot', right_on = 'StrongSeed')
    round6_df3 = pd.merge(round6_df,round_5_results, left_on = 'WeakSeed', right_on = 'Slot') 
    round6_df4 = pd.merge(round6_df2, round6_df3, on = 'StrongSeed')    
    
    round6_df4['Team1ID'] = round6_df4['Winner_x']
    round6_df4['Team2ID'] = round6_df4['Winner_y']
    round6_df20 = round6_df4[['Slot_x_x','Slot_x_y','Team1ID','Team2ID','Final Probability_x','Final Probability_y']]
    round6_df4 = round6_df4[['Slot_x_y','Team1ID','Team2ID']]
    
    
    round6_df20['Round'] = 6
    round6_df20 = pd.merge(round6_df20,df_teams, left_on = 'Team1ID', right_on = 'TeamID')
    round6_df20 = pd.merge(round6_df20, df_teams, left_on = 'Team2ID', right_on = 'TeamID')
    round6_df20['TeamName1'] = round6_df20['TeamName_x']
    round6_df20['TeamName2'] = round6_df20['TeamName_y'] 
    round6_df20 = round6_df20[['Team1ID','Team2ID','Round','TeamName1','TeamName2','Final Probability_x','Final Probability_y']]
    round6_df20 = round6_df20.append(round5_df20)
    
    
    
    round6_df5 = pd.merge(model_df, round6_df4, left_on  = 'TeamID',right_on = 'Team1ID')
    round6_df7 = pd.merge(round6_df5, model_df, left_on = 'Team2ID', right_on = 'TeamID')
    #get seed
    round6_df8 = get_seed[['Team1ID','Team1Seed']]
    round6_df9 = get_seed[['Team2ID','Team2Seed']]
    round6_df9['Team1ID'] = round6_df9['Team2ID']
    round6_df9['Team1Seed'] = round6_df9['Team2Seed']
    round6_df9 = round6_df9[['Team1ID','Team1Seed']]
    round6_df10 = round6_df8.append(round6_df9, sort = True, ignore_index = True)
    round6_df10['TeamSeedID'] = round6_df10['Team1ID']
    round6_df10 = round6_df10[['TeamSeedID','Team1Seed']]
    round6_df10 = round6_df10.drop_duplicates(subset='TeamSeedID', keep="last")
    
    #combine with seed
    round6_df11 = pd.merge(round6_df10, round6_df7, left_on = 'TeamSeedID', right_on = 'Team1ID')
    round6_df13 = pd.merge(round6_df11, round6_df10, left_on = 'Team2ID', right_on = 'TeamSeedID')
    round6_df13['Slot'] = round6_df13['Slot_x_y']
    
    #use this for slot
    round6_df12 = round6_df13[['Team1Seed_x', 'Average Pts_x',
       'Average PA_x', 'Home Record_x', 'Away Record_x', 'coach_win_pct_x',
       'orpg_x', 'orapg_x', 'avg_to_made_x', 'avg_to_allowed_x',
       'conf_win_pct_x', 'WSoS_x', 'LSoS_x', 'clutch_FtPct_x',
       'Slot', 'Team1ID', 'Team2ID',
       'Average Pts_y', 'Average PA_y', 'Home Record_y', 'Away Record_y',
       'coach_win_pct_y', 'orpg_y', 'orapg_y', 'avg_to_made_y',
       'avg_to_allowed_y', 'conf_win_pct_y', 'Seed_y', 'WSoS_y', 'LSoS_y',
       'clutch_FtPct_y', 'Team1Seed_y']]
    
    round6_df12['Seed_x'] = round6_df12['Team1Seed_x']
    round6_df12['Seed_y'] = round6_df12['Team1Seed_y']
    
    round6_df12['Average Pts'] = round6_df12['Average Pts_x'] - round6_df12['Average Pts_y']
    round6_df12['Average PA'] = round6_df12['Average PA_x'] - round6_df12['Average PA_y']
    round6_df12['Home Record'] = round6_df12['Home Record_x'] - round6_df12['Home Record_y']
    round6_df12['Away Record'] = round6_df12['Away Record_x'] - round6_df12['Away Record_y']
    round6_df12['Coach Record'] = round6_df12['coach_win_pct_x'] - round6_df12['coach_win_pct_y']
    round6_df12['offensive rebounds'] = round6_df12['orpg_x'] - round6_df12['orpg_y']
    round6_df12['offensive rbs allowed'] = round6_df12['orapg_x'] - round6_df12['orapg_y']
    round6_df12['avg_to_made'] = round6_df12['avg_to_made_x'] - round6_df12['avg_to_made_y']
    round6_df12['avg_to_allowed'] = round6_df12['avg_to_allowed_x'] - round6_df12['avg_to_allowed_y']
    round6_df12['conf win pct'] = round6_df12['conf_win_pct_x'] - round6_df12['conf_win_pct_y']
    round6_df12['Seed'] = round6_df12['Seed_x'] - round6_df12['Seed_y']
    round6_df12['WSoS'] = round6_df12['WSoS_x'] - round6_df12['WSoS_y']
    round6_df12['LSos'] = round6_df12['LSoS_x'] - round6_df12['LSoS_y']
    #left off here 1/1
    round6_df12['Clutch Ft Pct'] = round6_df12['clutch_FtPct_x'] - round6_df12['clutch_FtPct_y']
    final_bracket_round6 = round6_df12[['Average Pts','Average PA','Home Record','Away Record','Coach Record','offensive rebounds','offensive rbs allowed',
                                 'avg_to_made','avg_to_allowed','conf win pct','Seed','WSoS','LSos','Clutch Ft Pct']]


    return final_bracket_round6, round6_df13,round6_df20

def Winner():

    final_bracket_round6, bracket6,round6_df20 = round_6()
    
    round_6_results,bracket_7 = main(final_bracket_round6, bracket6)
    
    round_6_results['Winner'] = 1
    
    
    for i in range(0,1):
        if round_6_results.iloc[i,0] == 1:
            round_6_results.iloc[i,5] = round_6_results.iloc[i,1]
            
        
        if round_6_results.iloc[i,0] == 0:
            round_6_results.iloc[i,5] = round_6_results.iloc[i,2]
    
    round_6_results['Team1ID'] = round_6_results['Winner']
    round_6_results['Team2ID'] = 'N/A'
    round_6_results['Round'] = 7
    round_6_results = pd.merge(round_6_results, df_teams, left_on = 'Team1ID', right_on = 'TeamID')
    round_6_results['TeamName1'] = round_6_results['TeamName']
    round_6_results['TeamName2'] = 'N/A'
    round_6_results = round_6_results[['Team1ID','Team2ID','Round','TeamName1','TeamName2','Final Probability']]
    final_deliverable = round_6_results.append(round6_df20)
    #final_deliverable.to_excel("2018_results_prediction2018_2.xlsx")
    
    
    
           
    
    
    print(round_6_results)
    return round_6_results
    
    
    
    
    

    

    
    
    
    
      





































# =============================================================================
# c = b+1
#     d = b+2
#     e = b+3
# 
# 
#     for j in range(a):
#         if winner.iloc[j,b] < 0.5:
#             winner.iloc[j,c] = winner.iloc[j,0]
#             winner.iloc[j,d] = winner.iloc[j,1]
#             winner.iloc[j,e] = 1
# 
#         if winner.iloc[j,b] >= .5:
#             winner.iloc[j,c] = winner.iloc[j,1]
#             winner.iloc[j,d] = winner.iloc[j,0]
#             winner.iloc[j,e] = 0
#     
#     
#     
#     
#     
#             
#         
#     
#         
#         
#         
#     #return final_error_anal2, acc3, clf_meta
#     return clf_meta
#         
#         
# 
# #naive bayes
# 
# def error_analysis():
#     #change auto_final_df to the return on that function
#     df_test, acc4, clf_meta = ensemble()
#     y_pred1,y_test1 = ensemble()
#     y_pred1 = np.ravel(y_pred1)
#     y_test1 = np.ravel(y_test1)
#     error_df=pd.DataFrame(
#     {'prediction': y_pred1,
#      'actual': y_test1})
#     error_df['Error'] = error_df['prediction'] - error_df['actual']
#     size = len(error_df)
#     error_list = pd.DataFrame()
#     
# 
# #Test run-2018 Data!
# 
#     
#     for i in range(size):
#         if error_df.iloc[i,2] != 0:
#             error_list = error_list.append(auto_final_df.iloc[i,:])
#             #error_list2 = error_list2.append(error_df.iloc[i,0])
#         
#     #pd.merge(error_list, error_list2, how = 'Outer')
# 
#             
#             
# 
# 
# 
# 
# # =============================================================================
# # def test_model():
# #     dataset = set_up_model()
# #     X = dataset[['Average Pts','Average PA','Home Record','Away Record','Coach Record','offensive rebounds','offensive rbs allowed',
# #                                  'avg_to_made','avg_to_allowed','conf win pct','Seed','WSoS','LSos','Clutch Ft Pct']]
# #     y = dataset[['Output']]
# #     from sklearn.ensemble import RandomForestClassifier
# #     from sklearn.model_selection import train_test_split
# #     X_train, X_test, y_train, y_test = train_test_split(X,y, test_size =.2)
# #     classifier = RandomForestClassifier()
# #     
# #     classifier.fit(X_train, y_train)
# # 
# # #predicting the test set results
# #     y_pred = classifier.predict(X_test)
# #     from sklearn.metrics import classification_report, confusion_matrix
# #     rf_pred = pd.DataFrame(data = y_pred, columns = ["Random_Forest"])
# #     import sklearn
# #     accuracy = sklearn.metrics.accuracy_score(y_test, y_pred, normalize = True, sample_weight = None)
# #     d = {'RF': accuracy}
# #     rf_acc = pd.DataFrame(data = d, index = [0])
# #     
# #     return classifier
# # =============================================================================
# 
# 
#     #predictions = classifier.predict(final_bracket)
#     
#     
#     
#     length,width = round1.shape
#     for i in range(0,length):
#         
#     
#     
#     
#     
# 
#     
#     
#     
#     
#     
#     
#     
#     
# def run_through():
#     a, b, clf = ensemble()
# 
# 
# 
# =============================================================================









