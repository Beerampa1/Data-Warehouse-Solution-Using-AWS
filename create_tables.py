import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Droping tables if existed any from sparkifydb
    
    Parameters:
    * cur -- A cursor object connected to the sparkifydb database. This allows the execution of SQL commands.
    * conn -- A connection object (psycopg2) to the Postgres database (sparkifydb) in AWS Redshift.

    Returns:
        * The old tables in the sparkifydb database on AWS Redshift are dropped,effectively removing them from the database.
    """

    for query in drop_table_queries:
        try:
             cur.execute(query)
             conn.commit()
        
        except psycopg2.Error as e:
            print("Error: Issue dropping table: " + query)
            print(e)
    
    print("Tables dropped successfully.")

def create_tables(cur, conn):
    """
    Create new tables (songplays, users, artists, songs, time) to sparkifydb.
    
    Parameters:
           * cur -- A cursor object connected to the sparkifydb database. This allows the execution of SQL commands.
           * conn -- A connection object (psycopg2) to the Postgres database (sparkifydb) in AWS Redshift.

    Returns:
           * The new tables (songplays, users, artists, songs, time) are created in the sparkifydb database on AWS Redshift.
    """
    for query in create_table_queries:
        
        try:
            
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue creating table: " + query)
            print(e)
    
    print("Tables created successfully.")



def main():
    """
    Establish a connection to AWS Redshift, create a new database named sparkifydb, drop  any existing tables in the database, create new
    tables (songplays, users, artists, songs, time), and then close the database connection.
      
    Parameters (from dwh.cfg):
       * host -- The address of the AWS Redshift cluster.
       * dbname -- The name of the database.
       * user -- The username for accessing the database.
       * password -- The password associated with the user.
       * port -- The port number to connect to the database.
       * cur -- A cursor object connected to the sparkifydb database. This allows the  execution of SQL commands.
       * conn -- A connection object (psycopg2) to the Postgres database (sparkifydb) in AWSRedshift.

    Returns:
        * A new sparkifydb database is created.
        * Any existing tables in the sparkifydb database are dropped.
        * New tables (songplays, users, artists, songs, time) are created in the sparkifydb database.
    """


    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()