## Used to just run a temp query found in temp_query.sql

import sqlite3

def main():
    # Connect to the SQLite database
    conn = sqlite3.connect('..\\NBA_DB.db')
    cursor = conn.cursor()

    temp_query(conn,cursor)

    # Close connection
    conn.close()


"""
initialize tables form create_db.sql script (holds table names and all attribute data)
"""
def temp_query(conn,cursor):
    # Read and execute the SQL query from the file
    with open('temp_query.sql', 'r') as file:
        sql_query = file.read()
        cursor.executescript(sql_query)

    # Commit the changes
    conn.commit()



main()