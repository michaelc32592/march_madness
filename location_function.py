# -*- coding: utf-8 -*-
"""
Created on Wed Dec 26 14:08:34 2018

@author: micha
"""

#location
import pandas as pd
import psycopg2 as pg2
from sqlalchemy import create_engine

conn = pg2.connect(database = 'March_Madness' , user = 'postgres', password = 'crump83')
engine = create_engine('postgresql://postgres:crump83@localhost:5432/March_Madness')
cur = conn.cursor()



def location():
    sql51 = '''
    SELECT "WTeamID","LTeamID","CityID" FROM "GameCities"
    WHERE "CRType" = 'NCAA' AND "Seaon" = 2017
    
    '''
    df = pd.read_sql(sql51, conn)
    
    sql52 = '''
    SELECT * FROM "cities"
    
    '''
    df2 = pd.read_sql(sql52, conn)
    
    df3 = pd.merge(df, df2, on = "CityID")
    
    #get GPS of 'WTeamID'
    #get GPS of 'LTeamID'
    #look at what's closer and call that 'H'
    #pull home team home record and away team away record
    

