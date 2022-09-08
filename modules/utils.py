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


def write_transformed(df , destfolder='/home/rahul/reddit/posts_transformed/', dest_file='posts_',writeformat="csv" ):
    try:
        # df.to_csv( "home/rahul/reddit/transformed/"+dest_file,sep="|" )
        # df.to_csv( "C:\\Users\\kr716\\OneDrive\\Desktop\\newfol\\"+dest_file,sep="|" )
        if writeformat == "parquet":
            df.to_parquet( destfolder+dest_file+".parquet" )
        else:
            df.to_csv( destfolder+dest_file+".csv",sep="|" , index=False ,quotechar=".." )

    except Exception as e :
        raise e


def transformdata_raw(src_file,destfolder='/home/rahul/reddit/posts_transformed/',dtype=None):

    def changeedited_tobool(x):
        if x == "True":
            return True
        elif x == "False":
            return False
        else:
            return True

    def changeedited_tofloat(x):
        if x == "True":
            return 1
        elif x == "False":
            return 0
        else:
            return float(x)

    try:
        df = pd.read_csv(src_file,chunksize=300000,dtype=dtype ) 
        
        count_file = 1
        for data in df:
            #clean the dataframe
            #change the date in timestamp format 
            data["created_utc"] =  data["created_utc"].map(lambda x: pd.to_datetime(x,unit='s'))

            #change the bool/float column to float so that we can use it later
            data["edited"] = data["edited"].apply(changeedited_tofloat)
            data["over_18"] = data["over_18"].fillna(True)
            #remove data where there is nan in id or subreddit_id

            data["selftext"] = data["selftext"].replace("\n","" ).replace("\t","").replace("\r").replace("")
            data["title"] = data["title"].replace("\n","" ).replace("\t","").replace("\r")


            data = data.dropna(subset=['id', 'subreddit_id'])


            write_transformed(data,destfolder,"posts_"+str(count_file))
            count_file += 1

    except Exception as e:
        print(e)


def execute_insert_bulk(conn, table='posts' , insert_cols=['created_utc', 'score', 'domain', 'id', 'title', 'ups', 'downs',
       'num_comments', 'permalink', 'selftext', 'link_flair_text', 'over_18',
       'thumbnail', 'subreddit_id', 'edited', 'link_flair_css_class',
       'author_flair_css_class', 'is_self', 'name', 'url', 'distinguished'],filename=None): 
  
    cols = ','.join(insert_cols)
    # # SQL query to execute
    # query = """INSERT INTO %s(%s) VALUES %%s 
    # ON CONFLICT (id) DO UPDATE 
    #     SET score = excluded.score, 
    #         ups = excluded.ups,
    #         downs = excluded.downs""" % (table, cols)

    query = """COPY %s( %s )
FROM '%s'
DELIMITER '|'
CSV HEADER;""" % (table, cols,filename)

    cursor = conn.cursor()

    
    try:
        # extras.execute_values(cursor, query, tuples)
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()

def insert_to_STG(conn,src_folder):
    #create a conn to the database
    try:
        # conn = psycopg2.connect(database="redditdatabase", user='rahul', password='pass', host='127.0.0.1', port='5432')
        if conn == None:
            conn = psycopg2.connect(database="redditdatabase", user='rahul', password='Cherry@07', host='127.0.0.1', port='5432')
        
        print("connection to postgres successful")

        #read the csv file and insert it into database

        files = glob.glob(src_folder+"*.csv")
        for file in files:
            execute_insert_bulk(conn , filename=file )
        conn.close()
    except Exception as e:
        raise e
    finally :
        conn.close()

def execute_get(conn,query):

    
    try:
        df = pd.read_sql_query(query , conn)
        
        conn.commit()
        # conn.close()

        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.close()
        
        return 1

def execute_insert(conn, df, table): #this should also check for new rows or any change in existing rows
  
    tuples = [tuple(x) for x in df.to_numpy()]
    # print(df)
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

def check_change(df:pd.DataFrame,cols=["score","ups","downs"]) -> pd.DataFrame:
    
    
    try:
        df1 = df.apply(lambda x:sum([ x[i+"_x"]!= x[i+"_y"] for i in cols]),axis=1,result_type='expand')

        df = df.assign(change=df1)

        return df.loc[df1>0,["id"]+[x+"_x" for x in cols]].rename(columns={x+"_x":x for x in cols})

    except :
        return pd.DataFrame()

#not used
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
            write_transformed(str(dest_file+year+i) , df )
        conn.close()
        
    except Exception as e:
        raise e

def cdc_newdata_util(df_src,df_dest):
    #if there is a change in the
  try:
    print(df_src)
    print(df_dest)
    print(df_src["id"].to_numpy(),df_dest["id"].to_numpy())
    newids = np.setdiff1d(df_src["id"].to_numpy(),df_dest["id"].to_numpy(),assume_unique=True)
    # print(newids)
    df_new = df_src.loc[df_src["id"].isin(newids)]
    df_old = check_change(pd.merge(df_src,df_dest,on='id'))

    df_ins = pd.concat([df_old,df_new])

    return df_ins    
  except Exception as e:
    print(e,"in cdc")

def change_data_capture(conn,columns=["created_utc",'score','ups','downs','permalink','id','subreddit_id'] , dest_table='posts_2013' ):

    try:
        if not conn: 
            conn = psycopg2.connect(database="postgres", user='postgres', password='Cherry@07', host='127.0.0.1', port='5432')
            print("connection to postgres successful")
        
        columns = ','.join(columns)
        
        query = "select %s from %s where extract( year from created_utc ) = %s and extract( month from created_utc ) = %s"
        
        year = 2013
        for i in range(1,13):
            params = (columns , "posts" , year , i)
            print(query % params)
            df_src = execute_get(conn ,query % params )
            params = (columns , "posts_2013" , year , i)
            df_dest = execute_get(conn , query % params )
            try:
                df_ins = cdc_newdata_util(df_src,df_dest)
                execute_insert(conn,df_ins,dest_table)
            except Exception as e :

                print(f"something went wrong in month {i}" ,  e)
            
        conn.close()
        
    except Exception as e:
        raise e




        


