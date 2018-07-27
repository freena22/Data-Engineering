
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

