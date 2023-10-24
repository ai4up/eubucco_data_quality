import psycopg2
import geopandas as gpd
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import os

## The host
ssh_host = ('194.163.151.34', 22)
remote_bind = ('192.168.48.2', 5432)

## Access credentials
load_dotenv()
SSH_USER = os.getenv('SSH_USER')
SSH_PKEY = os.getenv('SSH_PKEY')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
DB = os.getenv('DB')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')


def SQL_to_parquet(query, chunksize=int(1e5), fpath="./data/parquet/test_", chunk_per_file=20):
    
    with SSHTunnelForwarder(ssh_address_or_host=ssh_host, ssh_username=SSH_USER, remote_bind_address=remote_bind,
                            ssh_pkey=SSH_PKEY, ssh_private_key_password=SSH_PASSWORD) as server:
        
        # 1. setup connection to Contabo VM
        server.start()
        print('Server connected.')

        params = {
            'database': DB,
            'user': DB_USER,
            'password': DB_PASS,
            'host': DB_HOST,
            'port': server.local_bind_port,
            'connect_timeout': 10,
            }

        # 2. connect to database and start query iterator
        conn = psycopg2.connect(**params)
        conn.set_session(readonly=True)
        
        data = gpd.read_postgis(
            sql=query,
            con=conn,
            geom_col='geometry', 
            index_col='id', 
            crs='EPSG:3035',
            chunksize=chunksize,
            )
        
        # 3. fetch data in chunks and write to file
        count = 1
        
        dfs = []
     
        for chunk in data:
            
            dfs.append(chunk)
                            
            total_row = chunksize*count
            print(f"{total_row} rows collected")
            
            if (count % chunk_per_file) == 0:
                fileid = count // chunk_per_file
                dfs = pd.concat(dfs)
                dfs.to_parquet(fpath + "{}.parquet".format(str(fileid).zfill(3)),  engine='pyarrow')
                
                print("Saving parquet file {}".format(fileid))
                dfs = []
                
                
            count += 1
        
        ### Leftover data to final file
        if dfs:
            fileid += 1
            dfs = pd.concat(dfs)
            dfs.to_parquet(fpath + "{}.parquet".format(str(fileid).zfill(3)),  engine='pyarrow')
            
        
        print("Done")
       
    return



def sql_to_df(query):
    
    with SSHTunnelForwarder(ssh_address_or_host=ssh_host, ssh_username=SSH_USER, remote_bind_address=remote_bind,
                            ssh_pkey=SSH_PKEY, ssh_private_key_password=SSH_PASSWORD) as server:
        
        # 1. connect to Contabo VM
        server.start()
        print('Server connected.')

        params = {
            'database': DB,
            'user': DB_USER,
            'password': DB_PASS,
            'host': DB_HOST,
            'port': server.local_bind_port,
            'connect_timeout': 10,
            }

        # 2. connect to database
        conn = psycopg2.connect(**params)
        conn.set_session(readonly=True)
        curs = conn.cursor()

        # 3. run query and save result to geodataframe
        data = gpd.GeoDataFrame.from_postgis(
            sql=query,
            con=conn,
            geom_col='geometry', 
            index_col='id', 
            )
        
    return data