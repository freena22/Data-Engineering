
################# Intro to Postgres #################

### 1. Connecting to Postgres

import psycopg2
conn = psycopg2.connect("dbname=dq user=dq")
print(conn)
conn.close()

### 2. Interacting with The Database

conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
cur.execute('SELECT * FROM notes')
notes = cur.fetchall()
conn.close()

### 3. Creating a Table / Drop a Table

conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
cur.execute("CREATE TABLE users(id integer PRIMARY KEY, email text, name text, address text)")
conn.commit() # to save the database state, need to commit that transaction
cur.execute("DROP TABLE IF EXISTS users")

### 4. Inserting the Data From a CSV file

import csv
with open('user_accounts.csv') as f:
    reader = csv.reader(f)
    next(reader)
    # skip the header row
    rows = [row for row in reader]
    
conn = psycopg2.connect("dbname = dq user=dq")
cur = conn.cursor()
for row in rows:
    cur.execute("INSERT INTO users VALUES (%s, %s, %s, %s)", row)
    # automatically converts each one of the types to the proper datatype expected by the table
conn.commit()

# check the result
cur.execute("SELECT * FROM users")
users = cur.fetchall()
conn.close()

### 5. Copying the Data

conn = psycopg2.connect('dbname=dq user=dq')
cur = conn.cursor()

# Use COPY FROM takes in a file (like a CSV) and automatically loads the file into a Postgres table
with open('user_accounts.csv', 'r') as f:
    # Skip the header row.
    next(f)
    cur.copy_from(f, 'users', sep=',')

cur.execute('SELECT * FROM users')
users = cur.fetchall()
conn.close()


################# Creating Tables With Proper Data Types #################


# 1. Describing a Table

conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
cur.execute('SELECT * FROM ign_reviews LIMIT 0')
print(cur.description)

# The desription property of the cursor outputs a tuple of column information from the table.


""" Output
(Column(name='id', type_code=20, display_size=None, internal_size=8, precision=None, scale=None, null_ok=None), 
 Column(name='score_phrase', type_code=1043, display_size=None, internal_size=11, precision=None, scale=None, null_ok=None), 
 Column(name='title', type_code=25, display_size=None, internal_size=-1, precision=None, scale=None, null_ok=None), 
 Column(name='url', type_code=25, display_size=None, internal_size=-1, precision=None, scale=None, null_ok=None), 
 Column(name='platform', type_code=1043, display_size=None, internal_size=20, precision=None, scale=None, null_ok=None), 
 Column(name='score', type_code=1700, display_size=None, internal_size=2, precision=2, scale=1, null_ok=None), 
"""

# 2. Adding the Id Field With Proper Type

# BIGINT type which can hold a maximum of 8 bytes, that is 64 bits 
# Where integer onlu holds 4 bytes
conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
cur.execute("""
    CREATE TABLE ign_reviews (
        id BIGINT PRIMARY KEY
    )
""")
conn.commit()

# 3. Get the Unique Content & Finding the Max length

import csv
with open('ign.csv', 'r') as f:
    next(f)
    reader = csv.reader(f)
    unique_words_in_score_phrase = set([
        row[1] for row in reader
    ])

print(unique_words_in_score_phrase)

""" Output
 {'Great', 'Mediocre', 'Bad', 'Good', 'Awful', 'Okay', 'Masterpiece', 'Amazing', 'Unbearable', 'Disaster', 'Painful'}
 """

# Finding the Max length

with open('ign.csv') as f:
    next(f)
    reader = csv.reader(f)
    len_characters_for_score = [
        len(row[1]) for row in reader
    ]
max_score = max(len_characters_for_score)

# 4. Creating the Other String Fields

# DECIMAL(3, 2) [1.23, 4.59, 10.2, 100]
​
# DECIMAL(2, 2) [1.2, 40, 0.2]
​
# DECIMAL(5, 1) [100.0, 63.1, 4000.1, 55555]

conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE ign_reviews (
        id BIGINT PRIMARY KEY,
        score_phrase VARCHAR(11),
        title TEXT,
        url TEXT,
        platform VARCHAR(20), # set the Varchar length
        score DECIMAL(3, 1), # set the precision
        genre TEXT,
        editors_choice BOOLEAN # set the boolean type
   )
""")
conn.commit()


# 5. Date Type

"""
Combine the columns into a single column using the datetime.date e.g. 2011-04-01

row = ['5249979066121302517', 'Amazing', 'LittleBigPlanet PS Vita', '/games/littlebigplanet-vita/vita-98907', 
'PlayStation Vita', '9.0', 'Platformer', 'Y', '2012', '9', '12']

"""
import csv
from datetime import date
cur.execute("""
    CREATE TABLE ign_reviews (
        id BIGINT PRIMARY KEY,
        score_phrase VARCHAR(11),
        title TEXT,
        url TEXT,
        platform VARCHAR(20),
        score DECIMAL(3, 1),
        genre TEXT,
        editors_choice BOOLEAN,
        release_date DATE
    )
""")
with open('ign.csv', 'r') as f:
    next(f)
    reader = csv.reader(f)
    for row in reader:
        updated_row = row[:8]
        updated_row.append(date(int(row[8]), int(row[9]), int(row[10])))
        cur.execute("INSERT INTO ign_reviews VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", updated_row)
        
conn.commit()        



################# Alter Table #################

# 1. Renaming Table Name

conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
cur.execute('ALTER TABLE old_ign_reviews RENAME TO ign_reviews')
conn.commit()

# 2. Removing a Column

cur.execute('ALTER TABLE ign_reviews DROP COLUMN full_url')
conn.commit()

# 3. Changing a Column Datatype

cur.execute('ALTER TABLE ign_reviews ALTER COLUMN id TYPE BIGINT')
conn.commit()

# 4. Renaming Columns

cur.execute('ALTER TABLE ign_reviews RENAME COLUMN title_of_game_review TO title')
conn.commit()

# 5. Adding a Column

cur.execute('ALTER TABLE ign_reviews ADD COLUMN release_date DATE')
conn.commit()


# 6. Set Values

# default each entry to Jan 1st, 1991
cur.execute("ALTER TABLE ign_reviews ADD COLUMN release_date DATE DEFAULT 01-01-1991")
# filter and categole
cur.execute("UPDATE ign_reviews SET editors_choice = 'F' WHERE id > 5000")
# 6. Update the Release Date

conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
# inserrt the combination of the columns || as concat
cur.execute("UPDATE ign_reviews SET release_date = to_date(release_day || '-' || release_month || '-' || release_year, 'DD-MM-YYYY')")
conn.commit()

# Drop the redundant columns
cur = conn.cursor()
cur.execute("ALTER TABLE ign_reviews DROP COLUMN release_day")
cur.execute("ALTER TABLE ign_reviews DROP COLUMN release_month")
cur.execute("ALTER TABLE ign_reviews DROP COLUMN release_year")


################# Loading and Extracting Data #################

# 1. Three Methods Which is the fastest?

import time

conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()

# Multiple single insert statements

start = time.time()
with open('ign.csv', 'r') as f:
    next(f)
    reader = csv.reader(f)
    for row in reader:
        cur.execute(
            "INSERT INTO ign_reviews VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            row
        )
conn.rollback()

print("Single statment insert: ", time.time() - start)
        
# Multiple mogrify insert

start = time.time()
with open('ign.csv', 'r') as f:
    next(f)
    reader = csv.reader(f)
    mogrified = [ 
        cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s)", row).decode('utf-8')
        for row in reader] 

    mogrified_values = ",".join(mogrified) 
    cur.execute('INSERT INTO ign_reviews VALUES ' + mogrified_values)
conn.rollback()
        
print("Multiple mogrify insert: ", time.time() - start)


# Copy expert method

start = time.time()
with open('ign.csv', 'r') as f:
    cur.copy_expert('COPY ign_reviews FROM STDIN WITH CSV HEADER', f)
conn.rollback()
print("Copy expert method: ", time.time() - start)

""" Output

Single statment insert:  2.5252981185913086
Multiple mogrify insert:  1.0149483680725098
Copy expert method:  0.15618491172790527

"""

conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
with open('old_ign_reviews.csv', 'w') as f:
    cur.copy_expert('COPY old_ign_reviews TO STDOUT WITH CSV HEADER', f)

# 2. Transform an Old Table to a New Table

import csv
from datetime import date


conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
with open('old_ign_reviews.csv', 'r+') as f:
    cur.copy_expert('COPY old_ign_reviews TO STDOUT WITH CSV HEADER', f)
    f.seek(0)
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        updated_row = row[:8]
        updated_row.append(date(int(row[8]), int(row[9]), int(row[10])))
        cur.execute("INSERT INTO ign_reviews VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", updated_row)
    conn.commit()

# copy_expert() method is great for tables that contain less than a million rows 
# As the table size increases, it requires even more memory and disk space to load and store these files.
# USE JUST SQL to do it!

conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
cur.execute("""
INSERT INTO ign_reviews (
    id, score_phrase, title, url, platform, score,
    genre, editors_choice, release_date
)
SELECT id, score_phrase, title_of_game_review as title,
    url, platform, score, genre, editors_choice,
    to_date(release_day || '-' || release_month || '-' || release_year, 'DD-MM-YYYY') as release_date
FROM old_ign_reviews
""")
conn.commit()

