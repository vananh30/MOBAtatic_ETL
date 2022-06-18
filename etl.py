import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
import json # data processing, json file I/O
from sqlalchemy import Table, Column, Float, Integer, String, MetaData, ForeignKey, JSON,Boolean
import sqlalchemy # copy pd dataframe to Redshift 
from sqlalchemy.types import * # load staging_tables
from sqlalchemy import create_engine
from functools import reduce 


def clean_json():
    print("JSON reading")
    meta_champs = pd.read_json('en_US-10.15.1/meta_champion.json')
    with open('en_US-10.15.1/meta_item.json') as f:
        data = json.load(f)
    meta_items = pd.read_json(json.dumps(data['data']), orient='index')
    # in the meta_dfs, we only need the key-name mapping, all other columns are dropped
    meta_items = meta_items[['name']]
    meta_champs['key'] = meta_champs.apply(lambda row: row.data['key'], axis=1)
    meta_champs = meta_champs[['key']]
    print("JSON read completed")
    return meta_items, meta_champs
def clean_pickle():
    print("Pickle reading")
    match_df = pd.read_pickle('archive/match_data_version1.pickle')
    winner_df =  pd.read_pickle('archive/match_winner_data_version1.pickle')
    loser_df = pd.read_pickle('archive/match_loser_data_version1.pickle')
    
    gameId_CLASSIC =  match_df[match_df['gameMode'] == 'CLASSIC'].gameId.tolist()
    winner_df = winner_df[winner_df['gameId'].isin(gameId_CLASSIC)]
    loser_df = loser_df[loser_df['gameId'].isin(gameId_CLASSIC)]
    match_df = match_df[match_df['gameId'].isin(gameId_CLASSIC)]
    
    gameId_remake = match_df.query('gameDuration <= 15*60').gameId.values.tolist()
    winner_df = winner_df[~winner_df['gameId'].isin(gameId_remake)]
    loser_df = loser_df[~loser_df['gameId'].isin(gameId_remake)]
    match_df = match_df[~match_df['gameId'].isin(gameId_remake)]
    print("Pickle reading Completed")
    return match_df, winner_df, loser_df
     
def inser_df(cur, df, inser_query, conn):
    for i in range(len(df)):
        cur.execute(inser_query, df[df.columns].values[i].tolist())
        conn.commit()
    
def process_table(cur, match_df, loser_df, winner_df, meta_items, meta_champs, conn):
    #### CHAMPION TABLE ####
    print("Processing CHAMPION tables")
    a = match_df[['gameId', 'participants']]
    df = a.explode('participants').reset_index(drop=True)
    df = df.merge(pd.json_normalize(df['participants']), left_index=True, right_index=True)
    
    champions_df = df[['gameId','participantId','championId'  ]]
    champions_df['idx'] = champions_df.groupby('gameId').cumcount()
    b = champions_df.pivot(index = 'gameId', columns='idx', values = 'championId').reset_index().rename_axis(columns=None, axis=1)
    is_20 = b[b[11].notnull()].gameId
    duplicate = a[a.duplicated('gameId')]
    is_dup = b[b.duplicated('gameId')].gameId
        #drop duplicate in gameid
    b = b.drop_duplicates(subset=['gameId'])
    #also exclude from orginal df
    df = df.drop_duplicates(subset=['gameId'])
    #drop column with contain chapm number is 20
    b = b[~b['gameId'].isin(is_20)]
    #also exclude from orginal df
    df = df[~df['gameId'].isin(is_20)]
    #just select column from 0->9
    c = b.iloc[:, 0:11]
    #just exclude the game that contain the number of champ < 10
    is_miss = c[c.isnull().any(axis=1)].gameId
    c = c[~c['gameId'].isin(is_miss)]
    #also exclude from orginal df
    df = df[~df['gameId'].isin(is_miss)]
    #convert type to valid int
    c = c.astype(int)
    #insert to postgres
    inser_df(cur, c, champions_table_insert, conn)
    
     #### ITEMS TABLE ####
    print("Processing ITEMS tables")
    items = ['gameId','participantId','stats.item0', 'stats.item1', 'stats.item2', 'stats.item3', 'stats.item4', 'stats.item5', 'stats.item6']
    item_df = df[items]
    df_items['combine'] = df_items[['stats.item0', 'stats.item1', 'stats.item2', 'stats.item3', 'stats.item4', 'stats.item5', 'stats.item6']].values.tolist()
    df_items = df_items[['gameId','participantId', 'combine']]
    df_items['idx'] = df_items.groupby('gameId').cumcount()
    df_items = df_items.pivot(index = 'gameId', columns='idx', values = 'combine').reset_index().rename_axis(columns=None, axis=1)
    df_items.gameId = df_items.gameId.astype(int)
    inser_df(cur, df_items, items_table_insert, conn)
    
     #### OBJECTIVES VISIONS TABLE ####
    print("Processing OBJECTIVES VISIONS tables")      
    win_badra= winner_df[['baronKills', 'dragonKills', 'gameId']].rename(columns = {'baronKills' : 'winbaronKills','dragonKills': 'windragonKills' })
    lose_badra= loser_df[['baronKills', 'dragonKills', 'gameId']].rename(columns = {'baronKills' : 'losebaronKills','dragonKills': 'losedragonKills' })
    badra = pd.merge( win_badra, lose_badra, on=['gameId'], how = 'inner').groupby('gameId').mean()
    col = ['stats.win','stats.wardsPlaced' , 'stats.wardsKilled', 'gameId','participantId' ]
    df_ov = df[col]
    df_false = df_ov[df_ov['stats.win'] == False]
    df_true = df_ov[df_ov['stats.win'] == True]
    lwp = df_false.groupby('gameId')['stats.wardsPlaced'].sum().to_frame().reset_index()
    wwp = df_true.groupby('gameId')['stats.wardsPlaced'].sum().to_frame().reset_index()
    lwk = df_false.groupby('gameId')['stats.wardsKilled'].sum().to_frame().reset_index()
    wwk = df_true.groupby('gameId')['stats.wardsKilled'].sum().to_frame().reset_index()
    data_list = [lwp,wwp,lwk, wwk, check]
    data_merge = reduce(lambda left, right: pd.merge(left , right, on = ["gameId"], how = "inner"), data_list)
    data_merge = pd.merge( data_merge, badra, on=['gameId'], how = 'inner').drop(columns =['participants'] )
    data_merge.drop(columns =['winbaronKills', 'losebaronKills', 'windragonKills', 'losedragonKills' ])
    data_merge.gameId = data_merge.gameId.astype(int)
    inser_df(cur, data_merge, objectives_visions_data, conn)
    
    #### champion_key TABLE ####
    print("Processing champion_key tables")  
    champion_key_df = meta_champs[['data']]
    champion_key_df = pd.json_normalize(champion_key_df['data'])
    champion_key_df = champion_key_df[['key', 'id']].sort_values(by=['key'])
    inser_df(cur, champion_key_df, champion_key_table_insert, conn)
   
    #### item_key TABLE ####
    print("Processing item_key tables")
    item_key_df = meta_items[['name']]
    item_key_df['index'] = item_key_df.index
    inser_df(cur, item_key_df, item_key_table_insert, conn)
    
    #### GAME TABLE ####
    print("Processing GAME tables")
    df_game = df[['gameId', 'player.accountId']] 
    df_game = df_game.groupby('gameId')['player.accountId'].apply(list).reset_index()
    a2 = df[['gameId', 'gameDuration', 'gameVersion']]
    df_game = pd.merge(df_game, a2, on = 'gameId', how = 'inner' )
    inser_df(cur, df_game, games_table_insert, conn)
    

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=MOBAtatic user=postgres password=123456")
    cur = conn.cursor()
    match_df, winner_df, loser_df = clean_pickle()
    meta_items, meta_champs = clean_json()
    
    print("Processing tables")
    process_table(cur, match_df, loser_df, winner_df, meta_items, meta_champs, conn)
    print("Completed!")
    
    conn.close()

if __name__ == "__main__":
    main() 
    
    
    

    
    