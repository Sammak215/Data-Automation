import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
# import psycopg2

from IPython.display import display

def remove_html_tags(text):
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text()

# Load the dataset
data = pd.read_json("amazon_reviews.json", lines=True)

to_drop = ['unixReviewTime', 'image']

df = pd.DataFrame(data)

# Drop columns with irrelvant data
df.drop(to_drop, inplace=True, axis=1)

# Drop rows with missing values
df.dropna(inplace=True)

# # Check for missing values
print("\nMissing Values:")
print(df.isnull().sum())

# Convert reviewTime to datetime format
df['reviewTime'] = pd.to_datetime(df['reviewTime'], format='%m %d, %Y')

# Normalize text fields (e.g., convert to lowercase)
df['reviewText'] = df['reviewText'].str.lower()

# Normalize text fields (e.g., convert to lowercase)
df['summary'] = df['summary'].str.lower()

# Extract key information (e.g., length of review)
df['reviewLength'] = df['reviewText'].apply(lambda x: len(x.split()))

# Extract key information (e.g., length of summary)
df['summaryLength'] = df['summary'].apply(lambda x: len(x.split()))

# Remove HTML-like tags from the 'reviewText' column
df['reviewText'] = df['reviewText'].apply(remove_html_tags)


# Display Data in Table form
display(df)

# Group by product ID and count the number of reviews for each product
product_reviews_count = df.groupby('asin').size().reset_index(name='review_count')

# Sort the products by review count in descending order and select the top 10
top_10_products = product_reviews_count.nlargest(10, 'review_count')

# Merge with the original dataset to get the average rating for each product
top_10_products_with_ratings = pd.merge(top_10_products, df.groupby('asin')['overall'].mean().reset_index(name='average_rating'), on='asin')

# Display the top 10 most reviewed products with their average ratings
display(top_10_products_with_ratings)



