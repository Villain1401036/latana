import imp
from itertools import count
from logging import log
import re
import requests
import csv
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras
import pandas.io.sql as sqlio
import os 
import glob

def alert(text):
    print(text)
    log(text)


#pull the data and store it in a specific location
def pull_data(url,dest_file):
    #pulls the data and store it in a file location
    try :
        res = requests.get(url)
        if res.status_code == 200:#on success
            try:
                open(dest_file , 'wb').write(res.content)
            except Exception as e:
                raise e
        elif res.status_code == 404:#not found
            alert("file not found. please check the url carefully")
        elif res.status_code/100 == 5: #bad gateway 
            alert("server is unavailable , bad gateway")
        else:
            alert("something went wrong")
    except Exception as e:
        raise e


def execute_insert(conn, df, table): #this should also check for new rows or any change in existing rows
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = """INSERT INTO %s(%s) VALUES %%s 
    ON CONFLICT (id) DO UPDATE 
        SET score = excluded.score, 
            ups = excluded.ups,
            downs = excluded.downs""" % (table, cols)

    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()
  
#insert the csvfile pulled in the pull_data in the postgresql after typechecking
async def insert_to_STG(src_folder,dtype_dict=None):
    #create a conn to the database
    try:
        conn = psycopg2.connect(database="redditdatabase", user='rahul', password='pass', host='127.0.0.1', port='5432')
        print("connection to postgres successful")

        #read the csv file and insert it into database

        files = glob.glob(src_folder)
        for file in files:
            df = pd.read_csv(file , dtype=dtype_dict)
            await  execute_insert(conn , df , "posts")
        # df = pd.read_csv(src_file,chunksize=30000 , dtype=dtype_dict)
        # for data in df:
        #    await  execute_insert(conn , data , "posts")
        conn.close()
    except Exception as e:
        raise e

def execute_get(conn,query,params):

    
    try:
        df = pd.read_sql_query(query , conn,params)
        
        conn.commit()
        conn.close()

        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        
        return 1

def write_transformed_csv(dest_file, df):
    try:
        df.to_csv( "home/rahul/reddit/transformed/"+dest_file,sep="|" )

    except Exception as e :
        raise e


def transformdata_raw(src_file,destfile,dtype=None):

    try:
        df = pd.read_csv(src_file,chunksize=300000,dtype=dtype ) 
        
        count_file = 1
        for data in df:
            #clean the dataframe

            write_transformed_csv(destfile+count_file+".csv",data)
            
    except :
        pass

def transformdata(dest_file,cols):
    #get the data from the source table 
    try:
        conn = psycopg2.connect(database="redditdatabase", user='rahul', password='pass', host='127.0.0.1', port='5432')
        print("connection to postgres successful")
        
        columns = ','.join(cols)
        
        query = "select %s from %s where year = %s and month = %s"
        year = 2013
        for i in range(1,13):
            params = [columns , "posts" , year , i]
            df = execute_get(query , conn, params)
            write_transformed_csv(str(dest_file+year+i) , df )
        conn.close()
        
    except Exception as e:
        raise e


def check_change(df:pd.DataFrame,cols=None) -> pd.DataFrame:
    
    cols = ["score","ups","downs"]
    try:
        df1 = df.apply(lambda x:sum([ x[i+"_x"]!= x[i+"_y"] for i in cols]),axis=1,result_type='expand')

        df = df.assign(change=df1)

        return df.loc[df1>0,["id"]+[x+"_x" for x in cols]].rename(columns={x+"_x":x for x in cols})

    except :
        return pd.DataFrame()


def cdc_newdata_util(df_src,df_dest):
    #if there is a change in the
    newids = np.setdiff1d(df_src,df_dest)
    df_new = df_src.loc[df_src["id"].isin(newids)]
    df_old = check_change(pd.merge(df_src,df_dest,on='id'))

    df_ins = pd.concat([df_new,df_old])

    return df_ins    

    
def change_data_capture(idcol=None,src_folder=None , dest_table=None ):

    try:
        conn = psycopg2.connect(database="redditdatabase", user='rahul', password='pass', host='127.0.0.1', port='5432')
        print("connection to postgres successful")
        
        columns = ','.join([""])
        
        querysrc = "select %s from %s where year = %s and month = %s"
        querydest =  "select %s from %s where year = %s and month = %s"
        year = 2013
        for i in range(1,13):
            params = [columns , "posts" , year , i]
            df_src = execute_get(querysrc , conn, params)
            params = [columns , "posts_2013" , year , i]
            df_dest = execute_get(querydest , conn, params)
            try:
                df_ins = cdc_newdata_util(df_src,df_dest)
                execute_insert(conn,df_ins,dest_table)
            except :
                print("something went wrong")
            
        conn.close()
        
    except Exception as e:
        raise e






        


