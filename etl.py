import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: Load JSON input data (log_data and song_data) from S3 and insert
                into staging_events and staging_songs tables.
        
    Input:
        cur:    reference to connected db.
        conn:   parameters (host, dbname, user, password, port) to connect Postgres database.
    Output:
        - Load log_data in staging_events table.
        - Load song_data in staging_songs table.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: Insert data from staging tables (staging_events and staging_songs)
                 into star schema analytics tables:
        - Fact table: songplays
        - Dimension tables: users, songs, artists, time
        
    Input:
        - cur: reference to connected db.
        - conn: parameters (host, dbname, user, password, port) to connect the DB.
        
    Output:
        - Insert data from staging tables to dimension tables.
        - Insert data from staging tables to fact table.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("AWS Redshift connection established")
    
    load_staging_tables(cur, conn)
    print("Data is loaded to Staging tables successfully")
    
    insert_tables(cur, conn)
    print("Data is inserted successfully")

    conn.close()


if __name__ == "__main__":
    main()