import sqlite3
import pandas as pd
import os
from log_helper import NFL_Logging
import inspect

def export_db_to_csv():
    log = NFL_Logging()
    log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)
    
    nfl_db = r'nfl_fantasy.db' # script runs adjacent to db location

    output_folder = r'NFL-CSV-Exports'

    # Connect to SQLite db
    conn = sqlite3.connect(nfl_db)
    cursor = conn.cursor()

    # Get list of all tables from db
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        # read table into pandas df
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

        # Save DF to csv
        csv_file = os.path.join(output_folder, f"{table_name}.csv")
        df.to_csv(csv_file, index=False)
        log.info(f"Exported {table_name} to {csv_file}. ")
    
    conn.close()


def main():
    export_db_to_csv()


if __name__ == "__main__":
    main()
        