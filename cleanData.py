import re

def clean_data(data):
  #convert evey word to lower case
  data = data.lower()
  #remove punctuations and newlinechar
  data = re.sub(r"newlinechar", " ", data)
  data = re.sub(r"i'm", "i am", data)
  data = re.sub(r"he's", "he is", data)
  data = re.sub(r"she's", "she is", data)
  data = re.sub(r"it's", "it is", data)
  data = re.sub(r"that's", "that is", data)
  data = re.sub(r"what's", "that is", data)
  data = re.sub(r"where's", "where is", data)
  data = re.sub(r"how's", "how is", data)
  data = re.sub(r"there's", "there is", data)
  data = re.sub(r"\'ll", " will", data)
  data = re.sub(r"\'ve", " have", data)
  data = re.sub(r"\'re", " are", data)
  data = re.sub(r"\'d", " would", data)
  data = re.sub(r"\'re", " are", data)
  data = re.sub(r"won't", "will not", data)
  data = re.sub(r"can't", "cannot", data)
  data = re.sub(r"n't", " not", data)
  data = re.sub(r"n'", "ng", data)
  data = re.sub(r"'bout", "about", data)
  data = re.sub(r"'til", "until", data)
  data = re.sub(r"[^ a-zA-Z0-9]", "", data)
  return data

if __name__ == '__main__':
  files = ['test.from', 'test.to']
  try :
    for file in files :
      with open('clean_{}'.format(file), 'a', encoding='utf8') as cf:
        with open('{}'.format(file),'r', encoding='utf8') as f:
          line_counter = 0
          for line in f:
            line = clean_data(line)
            cf.write(line + '\n')
            line_counter += 1
            if line_counter% 50000 == 0:
              print('{} lines completed in {}'.format(line_counter, file))
  except Exception as e:
    print(e)
    







          
