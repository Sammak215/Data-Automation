import psycopg2
import logging
from main import create_df_cleaned_data, most_reviewd_products


# Replace these values with your PostgreSQL connection parameters
host = 'cornelius.db.elephantsql.com'
dbname = 'ukjhmyqo'
user = 'ukjhmyqo'
password = 'm640wUQp4S8OB_vbWNYcK_CyR4n4fmT8'

# Connect to the PostgreSQL database
conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)

# Create a cursor object
cur = conn.cursor()

# Example: Create a table

def create_new_tables_in_postgres():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS Product_Reviews_Cleaned (review_id SERIAL PRIMARY KEY, overall INTEGER, verified BOOLEAN, review_time DATE, reviewer_id VARCHAR(20), asin VARCHAR(20), review_text TEXT, summary TEXT, vote INTEGER, review_length INTEGER, summary_length INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS Products_Most_Reviewed (asin VARCHAR(20) PRIMARY KEY, review_count INTEGER, average_rating NUMERIC(3, 2))""")
        logging.info("3 tables created successfully in Postgres server")
    except Exception as e:
        logging.error(f'Tables cannot be created due to: {e}')

def insert_product_reviews():
    query = "INSERT INTO Product_Reviews_Cleaned (overall, verified, review_time, reviewer_id, asin, review_text, summary, vote, review_length, summary_length) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    row_count = 0
    for index, row in create_df_cleaned_data().iterrows():
        values = (row['overall'], row['verified'], row['reviewTime'], row['reviewerID'], row['asin'], row['reviewText'], row['summary'], row['vote'], row['reviewLength'], row['summaryLength'])
        cur.execute(query, values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table Product_Reviews_Cleaned")


def insert_most_reviewed_products():
    query = "INSERT INTO Products_Most_Reviewed (asin, review_count, average_rating) VALUES (%s,%s,%s)"
    row_count = 0
    for index, row in most_reviewd_products().iterrows():
        values = (row['asin'], row['review_count'], row['average_rating'])
        # print(values)
        cur.execute(query, values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table Products_Most_Reviewed")




# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
# if __name__ == '__main__':
#     create_new_tables_in_postgres()
#     insert_product_reviews()

# # Reconnect to the database
# conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)

# # Create a cursor object
# cur = conn.cursor()

# # Example: Query the Product table
# cur.execute("SELECT * FROM Product")
# rows = cur.fetchall()
# for row in rows:
#     print(row)

# # Close the cursor and connection
# cur.close()
# conn.close()

