import os
from bs4 import BeautifulSoup
import requests
import time
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import io

# Step 1: Fetch HTML data
url = "https://en.wikipedia.org/wiki/List_of_most-streamed_songs_on_Spotify"

# Add User-Agent header to avoid 403 error
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
  print("Status:", response.status_code)

# Step 2: Extract tables with pandas
html = io.StringIO(response.text)
tables = pd.read_html(html)
print(f"{len(tables)} tables were found.\n")

# Get the first table
df = tables[0]
print(f"DataFrame shape: {df.shape}")
print(f"Columns: {list(df.columns)}\n")

# Step 3: Data cleaning
df = df.copy()

# Remove $ and B and strip whitespace
for col in df.columns:
    if df[col].dtype == 'object':
        df[col] = df[col].str.replace(r"[\$B,]", "", regex=True).str.strip()

print("Data cleaned successfully!\n")

# Step 4: Identify columns dynamically
streams_col = [col for col in df.columns if 'billions' in str(col).lower() or df.columns.tolist().index(col) == 3][0]
date_col = [col for col in df.columns if 'date' in str(col).lower() or 'released' in str(col).lower()][0]
artist_col = [col for col in df.columns if 'artist' in str(col).lower()][0]
song_col = [col for col in df.columns if 'song' in str(col).lower() or 'name' in str(col).lower()][0]
rank_col = [col for col in df.columns if 'rank' in str(col).lower()][0]

# Remove brackets and convert data types
for col in df.columns:
    if df[col].dtype == 'object':
        df[col] = df[col].str.replace(r"\[.*?\]", "", regex=True).str.strip()

df[streams_col] = pd.to_numeric(df[streams_col], errors='coerce')
df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
df[rank_col] = pd.to_numeric(df[rank_col], errors='coerce')
df['Year'] = df[date_col].dt.year

# Remove rows with missing streams
df = df.dropna(subset=[streams_col])

# Step 5: Save to CSV
csv_filename = "spotify_songs.csv"
df.to_csv(csv_filename, index=False)
print(f"Data saved to {csv_filename}")
print(f"Total rows exported: {len(df)}\n")

# Step 6: Create database and store data
conn = sqlite3.connect("spotify_songs.db")
print("Database created successfully!")

cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS songs (
    Rank INTEGER,
    Song TEXT,
    Artist TEXT,
    Streams_billions REAL,
    Date_released TEXT,
    Reference TEXT
)
""")
print("Table 'songs' created successfully!")

# Insert data
df.to_sql('songs', conn, if_exists='replace', index=False)
print(f"Inserted {len(df)} rows into the 'songs' table")

# Commit changes
conn.commit()
print("Changes committed successfully!")

# Verify data
cursor.execute("SELECT COUNT(*) FROM songs")
total_rows = cursor.fetchone()[0]
print(f"Total rows in database: {total_rows}\n")

conn.close()
print("Database connection closed.\n")

# Step 7: Visualize the data - 3 plots

# Plot 1: Artist vs Total Streams - Bar Chart
artist_streams = df.groupby(artist_col)[streams_col].sum().nlargest(10)
plt.figure(figsize=(12, 6))
plt.bar(range(len(artist_streams)), artist_streams.values, color='steelblue')
plt.xticks(range(len(artist_streams)), artist_streams.index, rotation=45, ha='right')
plt.xlabel("Artist")
plt.ylabel("Total Streams (billions)")
plt.title("Top 10 Artists by Total Streams (Bar Chart)")
plt.tight_layout()
plt.savefig('plot1_artists_bar.png')
print("Plot 1 saved as 'plot1_artists_bar.png'")
plt.close()

# Plot 2: Number of songs per year - Bar Chart
plt.figure(figsize=(10, 5))
sns.countplot(data=df, x="Year", order=sorted(df["Year"].dropna().unique()), palette="viridis")
plt.title("Number of Songs in the Ranking by Release Year")
plt.xlabel("Year")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('plot2_songs_by_year.png')
print("Plot 2 saved as 'plot2_songs_by_year.png'")
plt.close()

# Plot 3: Top 10 Songs by Rankings
top_songs = df.nsmallest(10, rank_col)
plt.figure(figsize=(12, 8))
plt.barh(range(len(top_songs)), top_songs[streams_col].values, color='coral')
plt.yticks(range(len(top_songs)), 
           [f"#{int(row[rank_col])} - {row[song_col][:30]}" for _, row in top_songs.iterrows()])
plt.xlabel("Streams (billions)")
plt.ylabel("Rank - Song")
plt.title("Top 10 Songs by Rankings")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig('plot3_top_songs.png')
print("Plot 3 saved as 'plot3_top_songs.png'")
plt.close()

print("\nAll tasks completed successfully!")
