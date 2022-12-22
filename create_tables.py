import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    """
    Description: Drop any existing tables from sparkifydb.
    
    Input:
        cur: reference to connected db.
        conn: parameters (host, dbname, user, password, port) to connect Postgres database.
        
    Output:
        - Old sparkifydb database tables are dropped from AWS Redshift.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: Create new tables (songplays, users, artists, songs, time) to sparkifydb.
    Input:
        cur: reference to connected db.
        conn: parameters (host, dbname, user, password, port) to connect Postgres database.
    Output:
        - New sparkifydb database tables are created into AWS Redshift.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    
#   Read Configration File
    config.read('dwh.cfg')
    
#   Connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("AWS Redshift connection established")
    
#   Drop table if exists
    drop_tables(cur, conn)
    
#   Create facts and dimensional tables
    create_tables(cur, conn)
    print("facts and dimensional tables Created")

    conn.close()


if __name__ == "__main__":
    main()