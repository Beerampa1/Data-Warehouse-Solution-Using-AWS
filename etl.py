import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Retrieve JSON input data (log_data and song_data) from Amazon S3 and insert it into two staging tables.
    
    
    Parameters:
         
         * cur -- A reference to a connected database (Postgres).
         * conn -- Parameters (host, dbname, user, password, port) required to establish a connection to the database.

    Returns:
         
    None. However, it loads the log_data in to staging_events table and the song_data into staging_songs table.
    """
   
    print("Start loading data from S3 to AWS Reshift tables...")

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("All files have been successfully copied.")

def insert_tables(cur, conn):
    """
    Insert data from the staging tables (staging_events and staging_songs) into the star schema analytics tables:
           
           * Fact table: songplays
           * Dimension tables: users, songs, artists, time

    
    Parameters:
          
            * cur -- A reference to a connected database (Postgres).
            * conn -- Parameters (host, dbname, user, password, port) required to establish a connection to the database.

    Returns:
            
     None.However, Data is inserted from the staging tables into the dimension tables (users, songs, artists, time) and fact table (songplays).
     """
    
    print("Start inserting data from staging tables into analysis tables...")
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
     
    print("All data has been successfully inserted.")


def main():
    
    """
    
    Establish a connection to the database and perform the following operations:

       Call the function load_staging_tables to load data from JSON files (song_data and log_data stored in Amazon S3) into the staging tables and insert_tables to insert data from the staging tables into the analysis tables (fact and dimension tables).
    
    Parameters:

                 None
    Returns:

    None, However, all input data is processed and loaded into the corresponding database tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("The connection to AWS Redshift has been successfully established.")
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()