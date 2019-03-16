# -*- coding: utf-8 -*-
"""
Created on Sun Dec  9 18:06:37 2018

@author: micha
"""
import pandas as pd
import psycopg2 as pg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

conn = pg2.connect(database = 'March_Madness' , user = 'postgres', password = 'crump83')
engine = create_engine('postgresql://postgres:crump83@localhost:5432/March_Madness')
cur = conn.cursor()



df = pd.read_sql('SELECT * FROM "cities"  ', conn)
df2 = pd.read_sql('SELECT * FROM "Conferences" ', conn)
df3 = pd.read_sql('SELECT * FROM "ConferenceTourneyGames" ', conn)
df4 = pd.read_sql('SELECT * FROM "NCAATourneyCompactResults" ', conn)
df5 = pd.read_sql('SELECT * FROM "NCAATourneyDetailedResults" ',conn)
df6 = pd.read_sql('SELECT * FROM "NCAATourneySeedRoundSlots" ', conn)
df7 = pd.read_sql('SELECT * FROM "NCAATourneySeeds" ', conn)
df8 = pd.read_sql('SELECT * FROM "NCAATOurneySlots" ', conn)
df9 = pd.read_sql('SELECT * FROM "RegularSeasonCompactResults" ', conn)
df10 = pd.read_sql('SELECT * FROM "RegularSeasonDetailedResults" ',conn)
df11 = pd.read_sql('SELECT * FROM  "Seasons" ', conn)
df12 = pd.read_sql('SELECT * FROM "SecondaryTourneyCompactResults"',conn)
df13 = pd.read_sql('SELECT * FROM "SecondaryTourneyTeams" ', conn)
df14 = pd.read_sql('SELECT * FROM "TeamCoaches" ', conn)
df15 = pd.read_sql('SELECT * FROM "Teams" ', conn)
#df2 = pd.read_sql('SELECT * FROM "WC_Two"  ', conn)
#df3 = pd.read_sql('SELECT * FROM "WC_Three"  ', conn)
#df4 = pd.read_sql('SELECT * FROM "WC_Four"  ', conn)



#figure out who won the most
#2017 analysis

#regular season success
sql = r'''

SELECT "WTeamID",COUNT("WTeamID")  FROM "RegularSeasonCompactResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
ORDER BY COUNT("WTeamID") DESC
LIMIT 10;
'''

#Winning team's seed in 2017 
sql5 = '''
SELECT "NCAATourneySeeds"."Seed","NCAATourneyDetailedResults"."WTeamID", "NCAATourneyDetailedResults"."Season" FROM "NCAATourneySeeds"

INNER JOIN "NCAATourneyDetailedResults" ON "NCAATourneySeeds"."TeamID" = "NCAATourneyDetailedResults"."WTeamID" AND 
"NCAATourneySeeds"."Season" = "NCAATourneyDetailedResults"."Season" 
WHERE "NCAATourneyDetailedResults"."Season" = 2017
'''

sql70 = '''
SELECT "TeamID", "Seed" FROM "NCAATourneySeeds"
WHERE "Season" = 2017
'''


seed2 = pd.read_sql(sql70,conn)
seed2 = seed2[['TeamID','Seed']]

sql50 = '''
SELECT "Seed", "TeamID" FROM "NCAATourneySeeds"
WHERE "Season" = 2017
'''

seed_l = pd.read_sql(sql50,conn)

#Losing Team Seed in 2017




#Average Team Stats (add rebounds, turnovers (both winner and loser), etc)
#WTO are offensive players

#winning team points scored Group By sort by desc
sql6 = '''
SELECT "WTeamID",AVG("WScore") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
ORDER BY  AVG("WScore") DESC
'''

points_scored_win = pd.read_sql(sql6, conn)


#winning team points allowed glroup by sort by desc
sql7 = '''
SELECT "WTeamID",AVG("LScore") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
ORDER BY  AVG("LScore") DESC
'''

points_scored_loss = pd.read_sql(sql7, conn)


#winning team rebounds
sql20 = '''
SELECT "WTeamID",AVG("WOR"+"WDR") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
ORDER BY  AVG("WOR"+"WDR") DESC
'''
w_rebounds = pd.read_sql(sql20,conn)

#do overall points (both winning and losing)
sql25 = '''
SELECT "WTeamID", SUM("WScore"),COUNT("WScore")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
'''
total_wpts = pd.read_sql(sql25,conn)
total_wpts['TeamID'] = total_wpts['WTeamID']
#losing sum/count

sql26 = '''
SELECT "LTeamID", SUM("LScore"),COUNT("LScore")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "LTeamID"
'''
total_lpts = pd.read_sql(sql26, conn)
total_lpts['TeamID'] = total_lpts['LTeamID']
total_avg_pts = pd.merge(total_wpts, total_lpts, how = 'inner', on = 'TeamID')
total_avg_pts['total_pts'] = total_avg_pts['sum_x'] + total_avg_pts['sum_y']
total_avg_pts['total_games'] = total_avg_pts['count_x'] + total_avg_pts['count_y']

total_avg_pts['ppg'] = total_avg_pts['total_pts']/total_avg_pts['total_games']
total_avg_pts['Average Pts'] = total_avg_pts['ppg']
total_avg_pts = total_avg_pts[['TeamID','Average Pts']]

#Points allowed
sql27 = '''
SELECT "WTeamID", SUM("LScore"),COUNT("LScore")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
'''
points_allowed_w = pd.read_sql(sql27,conn)
points_allowed_w['TeamID'] = points_allowed_w['WTeamID']

sql28 = '''
SELECT "LTeamID", SUM("WScore"),COUNT("WScore")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "LTeamID"
'''
points_allowed_l = pd.read_sql(sql28,conn)
points_allowed_l['TeamID'] = points_allowed_l['LTeamID']
total_avg_pa = pd.merge(points_allowed_w,points_allowed_l, how = 'inner', on = 'TeamID')
total_avg_pa['total_pts'] = total_avg_pa['sum_x'] + total_avg_pa['sum_y']
total_avg_pa['total_games'] = total_avg_pa['count_x'] + total_avg_pa['count_y']

total_avg_pa['Average PA'] = total_avg_pa['total_pts']/total_avg_pa['total_games']
total_avg_pa = total_avg_pa[['TeamID','Average PA']]


#join winning and losing sum columns and counts
#divide out


#ORB
sql29 = '''
SELECT "WTeamID", SUM("WOR"),COUNT("WOR")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
'''
o_rbs_w = pd.read_sql(sql29, conn)
o_rbs_w['TeamID'] = o_rbs_w['WTeamID']
sql30 = '''
SELECT "LTeamID", SUM("LOR"),COUNT("LOR")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "LTeamID"
'''

o_rbs_l = pd.read_sql(sql30, conn)
o_rbs_l['TeamID'] = o_rbs_l['LTeamID']
o_rbs_for = pd.merge(o_rbs_w,o_rbs_l, how = 'inner', on = 'TeamID')
o_rbs_for['total_rbs'] = o_rbs_for['sum_x'] + o_rbs_for['sum_y']
o_rbs_for['total_games'] = o_rbs_for['count_x'] + o_rbs_for['count_y']
o_rbs_for['orpg'] = o_rbs_for['total_rbs']/o_rbs_for['total_games']
o_rbs_for = o_rbs_for[['TeamID','orpg']]

#offensive rebounds allowed
sql31 = '''
SELECT "WTeamID", SUM("LOR"),COUNT("LOR")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
'''
o_rbs_a_w = pd.read_sql(sql31, conn)
o_rbs_a_w['TeamID'] = o_rbs_a_w['WTeamID']

sql32 = '''
SELECT "LTeamID", SUM("WOR"),COUNT("WOR")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "LTeamID"
'''
o_rbs_a_l = pd.read_sql(sql32,conn)
o_rbs_a_l['TeamID'] = o_rbs_a_l['LTeamID']
o_rbs_against = pd.merge(o_rbs_a_w,o_rbs_a_l, how = 'inner', on = 'TeamID')
o_rbs_against['total_rb_a'] = o_rbs_against['sum_x'] + o_rbs_against['sum_y']
o_rbs_against['total_games'] = o_rbs_against['count_x'] + o_rbs_against['count_y']
o_rbs_against['orapg'] = o_rbs_against['total_rb_a']/o_rbs_against['total_games']
o_rbs_against= o_rbs_against[['TeamID','orapg']]

#Do check-look at points for/points against games 


#Turnovers
#turnovers made
sql33 = '''
SELECT "WTeamID", SUM("LTO"),COUNT("LTO")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
'''
to_w = pd.read_sql(sql33, conn)
to_w['TeamID'] =to_w['WTeamID']
sql34 = '''
SELECT "LTeamID", SUM("WTO"),COUNT("WTO")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "LTeamID"
'''

to_l = pd.read_sql(sql34, conn)
to_l['TeamID'] = to_l['LTeamID']
to_made = pd.merge(to_w,to_l, how = 'inner', on = 'TeamID')
to_made['total_tos_made'] = to_made['sum_x'] + to_made['sum_y']
to_made['total_games'] = to_made['count_x'] + to_made['count_y']
to_made['avg_to_made'] = to_made['total_tos_made']/to_made['total_games']
to_made = to_made[['TeamID','avg_to_made']]

#turnovers against
sql35 = '''
SELECT "WTeamID", SUM("WTO"),COUNT("WTO")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
'''
to_against_w = pd.read_sql(sql35, conn)
to_against_w['TeamID'] =to_against_w['WTeamID']

sql36 = '''
SELECT "LTeamID", SUM("LTO"),COUNT("LTO")
FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "LTeamID"
'''

to_against_l = pd.read_sql(sql36, conn)
to_against_l['TeamID'] = to_against_l['LTeamID']
to_against = pd.merge(to_against_w,to_against_l, how = 'inner', on = 'TeamID')
to_against['total_tos_against'] = to_against['sum_x'] + to_against['sum_y']
to_against['total_games'] = to_against['count_x'] + to_against['count_y']
to_against['avg_to_allowed'] = to_against['total_tos_against']/to_against['total_games']
to_against = to_against[['TeamID','avg_to_allowed']]


#Home vs Away



#winning team home vs losing team home
sql8 = '''
SELECT "WTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017 AND "WLoc" = 'H'
GROUP BY "WTeamID"
ORDER BY COUNT("WLoc") DESC;
'''


sql9 = '''
SELECT "WTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017 AND "WLoc" = 'N'
GROUP BY "WTeamID"
ORDER BY COUNT("WLoc") DESC
;
'''
sql10 = '''
SELECT "WTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017 AND "WLoc" = 'A'
GROUP BY "WTeamID"
ORDER BY COUNT("WLoc") DESC
;
'''

sql60='''
SELECT "LTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017 AND "WLoc" = 'A'
GROUP BY "LTeamID"
ORDER BY COUNT("WLoc") DESC
;
'''
sql61 = '''
SELECT "LTeamID", COUNT("WLoc") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017 AND "WLoc" = 'H'
GROUP BY "LTeamID"
ORDER BY COUNT("WLoc") DESC
'''


home_wins = pd.read_sql(sql8, conn)
home_losses = pd.read_sql(sql60, conn)
neutral_wins = pd.read_sql(sql9, conn) 
neutral_wins.columns = ['WTeamID','Neutral']
away_wins = pd.read_sql(sql10, conn) 
away_losses = pd.read_sql(sql61, conn)



home_record = pd.merge(home_wins, home_losses, left_on = "WTeamID", right_on = "LTeamID")
home_record['Home Record'] = home_record['count_x']/(home_record['count_x'] + home_record['count_y'])
home_record['TeamID'] = home_record['WTeamID']
home_record = home_record[['TeamID','Home Record']]

away_record = pd.merge(away_wins, away_losses, left_on = "WTeamID", right_on = "LTeamID")
away_record['Away Record'] = away_record['count_x']/(away_record['count_x'] + away_record['count_y'])
away_record['TeamID'] = away_record['WTeamID']
away_record= away_record[['TeamID','Away Record']]

#do home record, away record
#build a function looking at location, only include the one that applies (ie away record if team is away)





#overall record
sql11 = '''
SELECT "WTeamID", COUNT("WTeamID") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "WTeamID"
ORDER BY COUNT("WTeamID") DESC
;
'''
wins = pd.read_sql(sql11, conn)
wins.columns = ['TeamID','Wins']

sql12 = '''
SELECT "LTeamID", COUNT("LTeamID") FROM "RegularSeasonDetailedResults"
WHERE "Season" = 2017
GROUP BY "LTeamID"
ORDER BY COUNT("LTeamID") DESC
;
'''
losses = pd.read_sql(sql12, conn)
losses.columns = ['TeamID','Losses']

record = pd.merge(wins,losses, on = "TeamID")
record["overall_win_pct"] = record["Wins"]/(record["Wins"] + record["Losses"])
record = record[['TeamID','overall_win_pct']]
#record.to_sql("2017 Record",engine) 





#strength of schedule (ie record of losing teams if they won and vice versa)

sql14 = r'''
SELECT "WTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."WTeamID", "2017 Record"."TeamID","2017 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2017 Record" ON "RegularSeasonDetailedResults"."LTeamID"="2017 Record"."TeamID"
WHERE "Season" = 2017) AS foo
GROUP BY "WTeamID"
ORDER BY AVG("win_pct") DESC
''' 

W_strength_of_schedule = pd.read_sql(sql14, conn)
W_strength_of_schedule['TeamID'] = W_strength_of_schedule['WTeamID']
W_strength_of_schedule['WSOS'] = W_strength_of_schedule['avg']
W_strength_of_schedule = W_strength_of_schedule[['TeamID','WSOS']]


sql15 = r'''
SELECT "LTeamID",AVG("win_pct") FROM (
SELECT "RegularSeasonDetailedResults"."Season", "RegularSeasonDetailedResults"."LTeamID", "2017 Record"."TeamID","2017 Record"."win_pct"
FROM "RegularSeasonDetailedResults"
INNER JOIN "2017 Record" ON "RegularSeasonDetailedResults"."WTeamID"="2017 Record"."TeamID"
WHERE "Season" = 2017) AS foo
GROUP BY "LTeamID"
ORDER BY AVG("win_pct") DESC
''' 

L_strength_of_schedule = pd.read_sql(sql15, conn)
L_strength_of_schedule['LSOS'] = L_strength_of_schedule['avg']
L_strength_of_schedule['TeamID'] = L_strength_of_schedule['LTeamID']
L_strength_of_schedule = L_strength_of_schedule[['TeamID','LSOS']]




#Coach Analysis
#Total Tourney Wins/Losses for Coaches
sql16 = r'''
SELECT "TeamCoaches"."CoachName",COUNT("NCAATourneyDetailedResults"."WTeamID")
FROM "TeamCoaches"
INNER JOIN "NCAATourneyDetailedResults"  ON "TeamCoaches"."TeamID" = "NCAATourneyDetailedResults"."WTeamID" AND "TeamCoaches"."Season" = "NCAATourneyDetailedResults"."Season"  
GROUP BY "CoachName"
ORDER BY "count" DESC
'''
coach_wins = pd.read_sql(sql16, conn)

sql17 = r'''
SELECT "TeamCoaches"."CoachName",COUNT("NCAATourneyDetailedResults"."LTeamID")
FROM "TeamCoaches"
INNER JOIN "NCAATourneyDetailedResults"  ON "TeamCoaches"."TeamID" = "NCAATourneyDetailedResults"."LTeamID" AND "TeamCoaches"."Season" = "NCAATourneyDetailedResults"."Season"  
GROUP BY "CoachName"
ORDER BY "count" DESC
'''
coach_losses = pd.read_sql(sql17, conn)

coach_record = pd.merge(coach_wins,coach_losses, on = "CoachName")
coach_record["coach_win_pct"] = coach_record["count_x"]/(coach_record["count_x"]+ coach_record["count_y"]) 
coach_record = coach_record[['CoachName','coach_win_pct']]

sql71 = r'''
SELECT * FROM "TeamCoaches" 
WHERE "Season" = 2017

'''
team_coach = pd.read_sql(sql71, conn)
team_coach_record = pd.merge(coach_record, team_coach, on = "CoachName")
team_coach_record = team_coach_record[['TeamID','coach_win_pct']]


#play by play
#efficiency
#reliability on one player

#winning clutch made fts
#MAKE SURE TO CHANGE TO 17 ONCE DOWNLOADED
sql22 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents17"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'made1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''

clutch_ft_made = pd.read_sql(sql22, conn)

sql23 = '''
SELECT "WTeamID",COUNT("EventType") FROM "PlaybyPlayEvents17"
WHERE "ElapsedSeconds" > 2200  AND "WTeamID" = "EventTeamID" AND "EventType" = 'miss1_free'
GROUP BY "WTeamID"
ORDER BY COUNT("EventType") DESC
'''
clutch_ft_miss = pd.read_sql(sql23, conn)

clutch_ft = pd.merge(clutch_ft_made,clutch_ft_miss, on = "WTeamID")
clutch_ft["clutch_FtPct"] = clutch_ft['count_x']/(clutch_ft['count_x']+ clutch_ft['count_y'])
clutch_ft['TeamID'] = clutch_ft['WTeamID']
clutch_ft = clutch_ft[['TeamID','clutch_FtPct']]

#Look at momentum as well (weight later wins more)
#Conference Record
sql40 = '''
SELECT "WTeamID",COUNT("WTeamID") FROM "ConferenceTourneyGames"
WHERE "Season" = 2017
GROUP BY "WTeamID"
ORDER BY COUNT("WTeamID") DESC
'''
conference_wins = pd.read_sql(sql40, conn)
#conference_wins['TeamID'] = conference_wins['WTeamID']
sql41 = '''
SELECT "LTeamID",COUNT("LTeamID") FROM "ConferenceTourneyGames"
WHERE "Season" = 2017
GROUP BY "LTeamID"
ORDER BY COUNT("LTeamID") DESC
'''
conference_losses = pd.read_sql(sql41, conn)
#conference_losses['TeamID'] = conference_losses["LTeamID"]

conference_record = pd.merge(conference_wins,conference_losses,how = 'left', left_on = "WTeamID", right_on = "LTeamID")
conference_record.fillna(0, inplace = True)
conference_record['conf_win_pct'] = conference_record['count_x']/(conference_record['count_x'] + conference_record['count_y'])
conference_record['TeamID'] = conference_record['WTeamID']
conference_record=conference_record[['TeamID','conf_win_pct']]

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
#seed_2: seed of 2017 tourney "TeamID", seel_d["Seed"]

#put everything together
#join everything by 'TeamID'
overall_df = pd.merge(total_avg_pts,total_avg_pa, how = 'left',on = 'TeamID').merge(home_record, how = 'left',on = 'TeamID').merge(away_record,how = 'left',on = 'TeamID').merge(
        W_strength_of_schedule,how = 'left',on = 'TeamID').merge(L_strength_of_schedule, how = 'left',on = 'TeamID').merge(team_coach_record,how = 'left',on = 'TeamID').merge(
        clutch_ft, how = 'left',on = 'TeamID').merge(o_rbs_for, how = 'left',on = 'TeamID').merge(o_rbs_against, how = 'left',on = 'TeamID').merge(to_made, how = 'left',on = 'TeamID').merge(
        to_against, how = 'left',on = 'TeamID').merge(conference_record,how = 'left', on = 'TeamID').merge(seed2,how = 'left', on = 'TeamID')
#fillna
overall_df["conf_win_pct"].fillna(0, inplace = True)
overall_df["coach_win_pct"].fillna(overall_df["coach_win_pct"].mean(), inplace = True)
overall_df["Home Record"].fillna(overall_df["Home Record"].mean(), inplace = True)
overall_df["Away Record"].fillna(overall_df["Away Record"].mean(), inplace = True)
overall_df['Seed'] = overall_df['Seed'].str[1:]
if overall_df['Seed'].isna == False:
    overall_df['Seed'] = overall_df['Seed'].astype(int)

#add in location once results are known

#calculate differences
#do half with 'LTeamID' first and randomize so output isn't always 1

#Set Up Tourney Schedule
#do first four Games
sql201 = '''
SELECT * FROM "NCAATOurneySlots" WHERE "Season" = 2017
'''
schedule = pd.read_sql(sql201,conn)

sql202 = r'''

SELECT * FROM "NCAATOurneySlots" WHERE "Season" = 2017 AND "StrongSeed" LIKE '%%a'


'''
#figure out what to do with this
cur.execute(sql202, conn)

strong_seed = pd.merge(seed2, schedule, left_on = 'Seed', right_on = 'StrongSeed')
strong_seed['High Seed'] = strong_seed['TeamID']

weak_seed = pd.merge(seed2, schedule, left_on = 'Seed', right_on = 'WeakSeed')
weak_seed['Low Seed'] = weak_seed['TeamID']

seeds = pd.merge(strong_seed, weak_seed, on = 'index')
seeds = seeds[['High Seed','StrongSeed_y','Low Seed','WeakSeed_y']]

sql203 = r'''

SELECT "WTeamID","LTeamID" FROM "NCAATourneyCompactResults"
WHERE "Season" = 2017


'''
#add in something about season
winner = pd.read_sql(sql203, conn)
winner_check = pd.read_sql(sql203,conn)

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
        
winner2 = winner[['Team1','Team2','Output']]

final_df_left = pd.merge(overall_df,winner2, left_on = 'TeamID',right_on = 'Team1')
final_df = pd.merge(final_df_left, overall_df, left_on = 'Team2', right_on = 'TeamID')

#pull out seed#





 




