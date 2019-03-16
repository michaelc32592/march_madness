# -*- coding: utf-8 -*-
"""
Created on Sun Dec  9 15:51:55 2018

@author: micha
"""

import pandas as pd

df = pd.read_csv("cities.csv")
df2 = pd.read_csv("Conferences.csv")
df3 = pd.read_csv("ConferenceTourneyGames.csv")
df4 = pd.read_csv("NCAATourneyCompactResults.csv")
df5 = pd.read_csv("NCAATourneyDetailedResults.csv")
df6 = pd.read_csv("NCAATourneySeedRoundSlots.csv")
df7 = pd.read_csv("NCAATourneySeeds.csv")
df8 = pd.read_csv("NCAATOurneySlots.csv")
df9 = pd.read_csv("RegularSeasonCompactResults.csv")
df10 = pd.read_csv("RegularSeasonDetailedResults.csv")
df11 = pd.read_csv("Seasons.csv")
df12 = pd.read_csv("SecondaryTourneyCompactResults.csv")
df13 = pd.read_csv("SecondaryTourneyTeams.csv")
df14 = pd.read_csv("TeamCoaches.csv")
df15 = pd.read_csv("Teams.csv")
#df16 = pd.read_csv("TeamSpellings.csv")
df17 = pd.read_csv("MasseyOrdinals.csv") 
df18 = pd.read_csv("Events_2018.csv")
df19 = pd.read_csv("Players_2018.csv")

#add in game cities!
df20 = pd.read_csv("GameCities.csv")
df21 = pd.read_csv("Events_2017.csv")
df22 = pd.read_csv("Players_2017.csv") 
df23 = pd.read_csv("Events_2016.csv")
df24 = pd.read_csv("Players_2016.csv")
df25 = pd.read_csv("Events_2015.csv")
df26 = pd.read_csv("Players_2015.csv")
df27 = pd.read_csv("Events_2014.csv")
df28 = pd.read_csv("Players_2014.csv")
df29 = pd.read_csv("Events_2013.csv")
df30 = pd.read_csv("Players_2013.csv")
df31 = pd.read_csv("Events_2012.csv")
df32 = pd.read_csv("Players_2012.csv")
df33 = pd.read_csv("Events_2011.csv")
df34 = pd.read_csv("Players_2011.csv")
df35 = pd.read_csv("Events_2010.csv")
df36 = pd.read_csv("Players_2010.csv")
df37 = pd.read_csv("March Madness.csv")
df38 = pd.read_csv("RegularSeasonDetailedResults_Prelim2018.csv")
df39 = pd.read_csv("TeamCoaches_Prelim2018.csv")
df40 = pd.read_csv("Events_2018.csv")



import psycopg2 as pg2
from sqlalchemy import create_engine



conn = pg2.connect(database = 'March_Madness' , user = 'postgres', password = 'crump83')
engine = create_engine('postgresql://postgres:crump83@localhost:5432/March_Madness')


#df.to_sql("cities", engine) 
#df2.to_sql("Conferences", engine) 
#df3.to_sql("ConferenceTourneyGames", engine)
#df4.to_sql("NCAATourneyCompactResults", engine) 
#df5.to_sql("NCAATourneyDetailedResults", engine)
#df6.to_sql("NCAATourneySeedRoundSlots",engine)
#df7.to_sql("NCAATourneySeeds",engine)
#df8.to_sql("NCAATOurneySlots",engine)
#df9.to_sql("RegularSeasonCompactResults",engine)
#df10.to_sql("RegularSeasonDetailedResults", engine)
#df11.to_sql("Seasons",engine)
#df12.to_sql("SecondaryTourneyCompactResults", engine)
#df13.to_sql("SecondaryTourneyTeams", engine)
#df14.to_sql("TeamCoaches",engine)
#df15.to_sql("Teams",engine)
#df16.to_sql("TeamSpellings",engine)
#df17.to_sql("MasseyOrdinals.csv",engine)
#df18.to_sql("PlaybyPlayEvents18",engine)
#df19.to_sql("PlaybyPlayPlayers18",engine)
#df20.to_sql("GameCities",engine)
#df21.to_sql("PlaybyPlayEvents17",engine)
#df22.to_sql("PlaybyPlayPlayers17",engine)
#df23.to_sql("PlaybyPlayEvents16", engine)
#df24.to_sql("PlaybyPlayPlayers16", engine) 
#df25.to_sql("PlaybyPlayEvents15", engine)
#df26.to_sql("PlaybyPlayPlayers15", engine) 
#df27.to_sql("PlaybyPlayEvents14", engine)
#df28.to_sql("PlaybyPlayPlayers14", engine) 
#df29.to_sql("PlaybyPlayEvents13", engine)
#df30.to_sql("PlaybyPlayPlayers13", engine)
#df31.to_sql("PlaybyPlayEvents12", engine)
#df32.to_sql("PlaybyPlayPlayers12", engine)
#df33.to_sql("PlaybyPlayEvents11", engine)
#df34.to_sql("PlaybyPlayPlayers11", engine)
#df35.to_sql("PlaybyPlayEvents10", engine)
#df36.to_sql("PlaybyPlayPlayers10", engine) 
#df37.to_sql("March Madness 18 Tourney",engine) 
df38.to_sql("RegularSeason18",engine)
df39.to_sql("Coaches18",engine)
df40.to_sql("PlaybyPlayEvents18",engine)



  
#a = 2017
#sql300 = r'''

#SELECT  * FROM "NCAATOurneySlots"


#'''
#df = pd.read_sql(sql300, conn)
#df = df[df['Season'] == a]

#print(df[df['W']>0])

#def func(b):
#    sql300 = r'''

#SELECT  * FROM "NCAATOurneySlots"


#'''
#    df2 = pd.read_sql(sql300, conn)
#    df2 = df2[df2['Season'] == b]
#    return df2

#func(2017)

sql = r'''
SELECT * FROM "RegularSeasonDetailedResults"

'''
df50 = pd.read_sql(sql, conn)

sql2 = r'''
SELECT * FROM "RegularSeason18"
WHERE "Season" = 2018
'''
df51 = pd.read_sql(sql2, conn)

#df50 = df50.append(df51)

#DROP TABLES

sql1000 = r'''

DROP TABLE "cities"; commit;

'''

#cur.execute(sql1000, conn)

sql1001 = r'''

DROP TABLE "Conferences"; commit;


'''
sql1002 r'''

DROP TABLE "ConferenceTourneyGames"; commit;

'''

sql1011 = r'''

DROP TABLE "NCAATourneyCompactResults"; commit;

'''


sql1003 = r'''

DROP TABLE "RegularSeasonDetailedResults"; commit;

'''

sql1004 =  r'''

DROP TABLE "NCAATourneySeedRoundSlots"; commit;

'''

sql1005 =  r'''

DROP TABLE "NCAATourneySeeds"; commit;

'''

sql1006 =  r'''

DROP TABLE "NCAATOurneySlots"; commit;

'''

sql1007 =  r'''

DROP TABLE "RegularSeasonCompactResults"; commit;

'''

sql1008 =  r'''

DROP TABLE "RegularSeasonDetailedResults"; commit;

'''

sql1009 =  r'''

DROP TABLE "TeamCoaches"; commit;

'''

sql1010 =  r'''

DROP TABLE "ConferenceTourneyGames"; commit;

'''