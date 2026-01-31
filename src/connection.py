import psycopg2

def get_connection():
    conn = psycopg2.connect(
        database="watcherdb", user="yl762", host="127.0.0.1", port="5555"
    )
    conn.set_session(autocommit=True)
    cursor = conn.cursor()
    return conn, cursor

def close_connection(conn, cursor):
    cursor.close()
    conn.close()
