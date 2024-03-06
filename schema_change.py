import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname="your_database_name",
    user="your_username",
    password="your_password",
    host="your_host_address",
    port="your_port_number"
)
cur = conn.cursor()

# Define the migration script to add a new column
migration_script = """
ALTER TABLE reviews ADD COLUMN sentiment_analysis_result TEXT;
"""

# Execute the migration script
cur.execute(migration_script)
conn.commit()

# Perform data migration by populating the new column with default values
# You may replace this with your actual sentiment analysis results
update_script = """
UPDATE reviews SET sentiment_analysis_result = 'positive' WHERE overall > 3;
UPDATE reviews SET sentiment_analysis_result = 'neutral' WHERE overall = 3;
UPDATE reviews SET sentiment_analysis_result = 'negative' WHERE overall < 3;
"""

cur.execute(update_script)
conn.commit()

# Verify data integrity
cur.execute("SELECT * FROM reviews LIMIT 10")
rows = cur.fetchall()
for row in rows:
    print(row)

# Close the cursor and connection
cur.close()
conn.close()
