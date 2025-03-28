import sqlite3
import pandas as pd
from datetime import datetime, timedelta

csvfile = "databases/insurance_claims_event_log.csv"

def create_event_log_table_from_schema(conn, schema_source):
    print("Creating event_log table...")
    
    if isinstance(schema_source, pd.DataFrame):
        # If source is a DataFrame, create table directly from DataFrame
        schema_source.head(0).to_sql('event_log', conn, index=False, if_exists='replace')
    else:
        # If source is a connection, get schema from existing table
        cursor = schema_source.cursor()
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='event_log'")
        create_stmt = cursor.fetchone()[0]
        cursor.execute(create_stmt)
    
    conn.commit()
    print("Table created successfully!")

def create_and_populate_table_from_csv(conn):
    print("Creating and populating event_log table from CSV...")
    try:
        # Read the CSV file
        data = pd.read_csv(csvfile)
        
        # Create and populate the table
        data.to_sql('event_log', conn, index=False, if_exists='replace')
        print("Table created and populated successfully!")
        return data
        
    except FileNotFoundError:
        print(f"Error: CSV file {csvfile} not found in current directory")
        return None
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        return None

def adjust_timestamps(days_adjustment):
    print("Starting timestamp adjustment...")
    
    try:
        print("Connecting to original database...")
        conn = sqlite3.connect('process_mining.db')
        
        # Check if table exists
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='event_log'
        """)
        
        if not cursor.fetchone():
            print("Error: Table 'event_log' not found. Please create the database first using option 1 in the main menu.")
            conn.close()
            return
        
        print("Reading data from database...")
        data = pd.read_sql_query("SELECT * FROM event_log", conn)
        
        print(f"Adjusting timestamps by {days_adjustment} days...")
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data['timestamp'] = data['timestamp'] + timedelta(days=days_adjustment)
        
        print("Creating new database...")
        new_db_name = f'event_log_adjusted_{days_adjustment}_days.db'
        
        new_conn = sqlite3.connect(new_db_name)
        
        print("Writing to new database...")
        data.to_sql('event_log', new_conn, index=False, if_exists='replace')
        new_conn.close()
        conn.close()
        
        print("Adjustment complete!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        if 'conn' in locals():
            conn.close()
        if 'new_conn' in locals():
            new_conn.close()

def main():
    while True:
        try:
            days = int(input("Select value to adjust timestamps:"))
            if days == 0:
                print("Exiting program")
                break
            adjust_timestamps(days)
        except ValueError:
            print("Please enter a valid number")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    print("Script started...")
    main()
    print("Script finished!") 