import sqlite3

def main():
    # Connect to the SQLite database
    conn = sqlite3.connect('..\\nfl_fantasy.db')
    cursor = conn.cursor()

    delete_current_data(conn, cursor)
    init_tables(conn, cursor)
    init_teams_table(conn, cursor)


    # Close connection
    conn.close()


"""
Grab current table names and drop them all 
"""
def delete_current_data(conn, cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Drop each table
    for table in tables:
        table_name = table[0]
        if table_name != 'sqlite_sequence':
            conn.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Commit the changes
    conn.commit()

"""
initialize tables form create_db.sql script (holds table names and all attribute data)
"""
def init_tables(conn, cursor):
    # Read and execute the SQL query from the file
    with open('create_db.sql', 'r') as file:
        sql_query = file.read()
        cursor.executescript(sql_query)

    # Commit the changes
    conn.commit()


def init_teams_table(conn, cursor):
    # Reset teams table after db dropped with init_teams.sql
    with open('init_teams.sql', 'r') as file:
        sql_query = file.read()
        cursor.executescript(sql_query)

    # Commit the changes
    conn.commit()


main()