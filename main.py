import sqlite3
import pandas as pd
from adjust_timestamps import adjust_timestamps
from compare_databases import compare_databases


# Main script with the possibility to create a new database from a CSV file and
# modify it.

def create_database_from_csv(csv_path, db_name='databases/process_mining.db'):
    """Create a new database and populate it from a CSV file"""
    try:
        print(f"Reading CSV file: {csv_path}")
        data = pd.read_csv(csv_path)
        
        print(f"Creating database: {db_name}")
        conn = sqlite3.connect(db_name)
        
        print("Creating and populating event_log table...")
        data.to_sql('event_log', conn, index=False, if_exists='replace')
        
        conn.close()
        print("Database created successfully!")
        return True
        
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_path}' not found")
        return False
    except Exception as e:
        print(f"Error creating database: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return False

def display_menu():
    """Display the main menu options"""
    print("\nProcess Mining Database Operations")
    print("1. Create new database from CSV")
    print("2. Adjust timestamps")
    print("3. Compare databases")
    print("0. Exit")
    return input("Select an option: ")

def main():
    while True:
        choice = display_menu()
        
        if choice == "1":
            csv_path = input("Enter CSV file path (default: 'Insurance_claims_event_log.csv'): ").strip()
            if not csv_path:
                csv_path = 'Insurance_claims_event_log.csv'
            
            db_name = input("Enter database name (default: 'process_mining.db'): ").strip()
            if not db_name:
                db_name = 'process_mining.db'
                
            create_database_from_csv(csv_path, db_name)
            
        elif choice == "2":
            try:
                days = int(input("Enter number of days to adjust timestamps: "))
                adjust_timestamps(days)
            except ValueError:
                print("Please enter a valid number of days")
                
        elif choice == "3":
            original_db = input("Enter original database name (default: 'process_mining.db'): ").strip()
            if not original_db:
                original_db = 'process_mining.db'
            
            adjusted_db = input("Enter adjusted database name to compare: ").strip()
            if not adjusted_db:
                print("Error: Please provide the adjusted database name")
                continue
            
            compare_databases(original_db, adjusted_db)
        
        elif choice == "0":
            print("Goodbye!")
            break
            
        else:
            print("Invalid option, please try again")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main() 