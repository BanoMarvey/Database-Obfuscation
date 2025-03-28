import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# SQLite database connection
db_path = "process_mining.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

data = pd.read_sql("SELECT case_id, activity_name, timestamp FROM claims_process", conn)
print(data.at[47, "timestamp"])
# Convert timestamps to datetime format
data["timestamp"] = pd.to_datetime(data["timestamp"])

i = 0
while i < len(data):
    data.at[i, "timestamp"] = data.at[i, "timestamp"] + timedelta(days=1)
    i += 1

print(data.at[47, "timestamp"])
#print(df["timestamp"])

#cursor.execute("SELECT DISTINCT claimant_name FROM claims_process")

activities = cursor.fetchall()

data.to_sql("new_table_+1_timestamps", conn, if_exists="replace", index=False)

# Create a new database connection for the new database
new_db_path = "Add1DayToTimestamps.db"
new_conn = sqlite3.connect(new_db_path)

# Save the data to the new database
data.to_sql("claims_process", new_conn, if_exists="replace", index=False)

# Close both connections
conn.close()
new_conn.close()