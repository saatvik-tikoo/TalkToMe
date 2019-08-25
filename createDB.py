import sqlite3 as sql
import json
from datetime import datetime as dt
timeframe = '2017-11'
conn = ''
cur = ''
clean_counter = 100000
sql_txn = []

#Create a connection
def create_connection():
  global conn
  global cur
  try:
    conn = sql.connect('{}.db'.format(timeframe))
    print('Connection Successful')
    cur = conn.cursor()
  except Exception as e:
    print(e)

#Create the table
def create_table():
  try:
    sql = '''Create Table If not Exists parent_reply
          (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE,
          parent TEXT, comment TEXT, subreddit TEXT, unix INT,
          score INT)'''
    cur.execute(sql)
  except Exception as e:
    print(e)

#Format comment data
def remove_newline(data):
  data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"',"'")
  return data

#Fetch the comment
def get_parent(parent_id):
  try:
    sql = 'Select comment from parent_reply Where comment_id = "{}" Limit 1'.format(parent_id)
    cur.execute(sql)
    res = cur.fetchone()
    if res != None:
      return res[0]
    else:
      return False
  except Exception as e:
    print (e)
    return False

#Check if data is fit to be used
def data_acceptable(data):
  if len(data) > 1000 or len(data) < 1 or len(data.split(' ')) > 50:
    return False
  elif data == '[deleted]' or data == '[removed]':
    return False
  else:
    return True

#Score of the existing comment to a parent
def find_existing_score(parent_id):
  try:
    sql = 'SELECT score FROM parent_reply WHERE parent_id = "{}" LIMIT 1'.format(parent_id)
    cur.execute(sql)
    res = cur.fetchone()
    if res != None:
      return res[0]
    else:
      return False
  except Exception as e:
    print(e)
    return False

#Run multiple queries together as a transaction
def txn_bldr(sql):
  global sql_txn
  sql_txn.append(sql)
  if len(sql_txn) > 1000:
    cur.execute('BEGIN TRANSACTION')
    for query in sql_txn:
      try:
        cur.execute(query)
      except Exception as e:
        #print (e)
        pass
    conn.commit()
    sql_txn = []
#Update the existing comment
def update_comment(comment_id, parent_id, parent, comment, subreddit, time, score):
  try:
    sql = '''UPDATE parent_reply
          SET
          parent_id = "{}", comment_id = "{}",
          parent = "{}", comment = "{}",
          subreddit = "{}", unix = {}, score = {}
          WHERE parent_id ="{}";'''.format(parent_id, comment_id, parent, comment, subreddit, int(time), score, parent_id)
    txn_bldr(sql)
  except Exception as e:
    print(e)

#Insert the comment as a child
def insert_with_parent(comment_id, parent_id, parent, comment, subreddit, time, score):
  try:
    sql = '''INSERT INTO parent_reply
          (parent_id, comment_id, parent, comment, subreddit, unix, score)
          VALUES ("{}","{}","{}","{}","{}",{},{});'''.format(parent_id, comment_id, parent, comment, subreddit, int(time), score)
    txn_bldr(sql)
  except Exception as e:
    print(e)

#Insert the body with no parent
def insert_with_no_parent(comment_id, parent_id, comment, subreddit, time, score):
  try:
    sql = '''INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score)
          VALUES ("{}","{}","{}","{}",{},{});'''.format(parent_id, comment_id, comment, subreddit, int(time), score)
    txn_bldr(sql)
  except Exception as e:
    print(e)

if __name__ == '__main__':
  create_connection()
  create_table()
  row_cntr = 0;
  pair_cntr = 0;
  with open('RC_{}'.format(timeframe), buffering = 1000) as f:
    for row in f:
      row_cntr += 1
      row = json.loads(row)
      parent_id = row['parent_id'].split('_')[1]
      comment_id = row['id']
      body = remove_newline(row['body'])
      subreddit = row['subreddit']
      created_utc = row['created_utc']
      score = row['score']
      parent_data = get_parent(parent_id)
      if score and score >= 5:
        if data_acceptable(body):
          existing_score = find_existing_score(parent_id)
          if existing_score:
            if existing_score < score:
              update_comment(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
          else:
            if parent_data:
              insert_with_parent(comment_id, parent_id, parent_data, body, subreddit,created_utc,score)
              pair_cntr += 1
            else:
              insert_with_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
                
      if row_cntr % clean_counter == 0:
        print ('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_cntr, pair_cntr, str(dt.now())))
        print("Cleaning up!")
        sql = "DELETE FROM parent_reply WHERE parent IS NULL"
        cur.execute(sql)
        conn.commit()
        cur.execute("VACUUM")
        conn.commit()
        print("Cleaning Done!")












          
