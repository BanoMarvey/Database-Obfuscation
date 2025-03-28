import sqlite3
import pandas as pd

# Load CSV file
csv_file = "insurance_claims_event_log.csv"  # Change this to your CSV filename
df = pd.read_csv(csv_file)

# Create a SQLite database connection
db_path = "process_mining.db"
conn = sqlite3.connect(db_path)

# Store the data in an SQL table
table_name = "claims_process"  # Name your table
df.to_sql(table_name, conn, if_exists="replace", index=False)

# Verify insertion
cursor = conn.cursor()
cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
print(f"Total records in {table_name}: ", cursor.fetchone()[0])

# Close the connection
conn.close()