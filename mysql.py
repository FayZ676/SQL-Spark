import json, mysql.connector, pandas as pd
from sqlalchemy import create_engine
from urllib.request import urlopen

# Credentials to database connection
hostname="localhost"
dbname="SE513"
uname="uname"
pwd="pwd"

# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))
con = engine.connect()


str1 = "https://storage.googleapis.com/projectse413/wikiviewpages/wikifile_0000000"
str4 = ".json"
number = 0

while number != 13037:
    # Create URL string
    str2 = str(number)
    str3 = str2.zfill(5)
    url = str1+str3+str4

    response = urlopen(url)
    lines = response.read().splitlines()

    df_inter = pd.DataFrame(lines)
    df_inter.columns = ['json_object']
    df_inter['json_object'].apply(json.loads)
    df_final = pd.json_normalize(df_inter['json_object'].apply(json.loads))
    # print("Finished reading wiki file ")

    # Convert dataframe to sql table using .to_sql
    df_final.to_sql(name='json', con=con, index=False, if_exists="append")

    print(number)
    number = number + 1

print("Process Complete!")
con.close()

# Use SQL Command: 
# "select title, count(title) as Occurrence from json where title='Keyword';"
# to search for keywords in the SQL table.
