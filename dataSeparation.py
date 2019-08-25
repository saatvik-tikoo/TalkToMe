import sqlite3 as sql
import pandas as pd

timeframe = '2017-11'
conn = ''
cur = ''

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

#Divide the data into train and test set
def segregate():
  test_set_size = 10000
  last_unix = 0
  cur_length = test_set_size
  counter = 0
  test_data = True

  while cur_length == test_set_size:
    sql = '''SELECT * FROM parent_reply
          WHERE unix > {} AND
          parent NOT NULL
          ORDER BY unix ASC LIMIT {}'''.format(last_unix, test_set_size)    
    df = pd.read_sql(sql, conn)
    last_unix = df.tail(1)['unix'].values[0]
    cur_length = len(df)
    if test_data:
      with open('test.from','a', encoding='utf8') as f:
        for content in df['parent'].values:
          f.write(content + '\n')

      with open('test.to','a', encoding='utf8') as f:
        for content in df['comment'].values:
          f.write( content + '\n')
        test_data = False

    else:
      with open('train.from','a', encoding='utf8') as f:
        for content in df['parent'].values:
          f.write(content + '\n')

      with open('train.to','a', encoding='utf8') as f:
        for content in df['comment'].values:
          f.write(str(content)+'\n')

    counter += 1
    if counter % 20 == 0:
      print(counter*test_set_size,'rows completed so far')

if __name__ == '__main__':
  create_connection()
  segregate()
