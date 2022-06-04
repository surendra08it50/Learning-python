from sqlalchemy import create_engine

#db_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
db_string = "postgresql+psycopg2://postgres:admin@localhost:5432/surendra"
engine = create_engine(db_string)

connection = engine.connect()

# Create 
connection.execute("CREATE TABLE IF NOT EXISTS films (title text, director text, year text)")  
connection.execute("INSERT INTO films (title, director, year) VALUES ('Doctor Strange', 'Scott Derrickson', '2016')")

# Read
result_set = connection.execute("SELECT * FROM films")  
for r in result_set:  
    print(r[0],r[1],r[2])

# Update
# connection.execute("UPDATE films SET title='Some2016Film' WHERE year='2016'")

# Delete
#connection.execute("DELETE FROM films WHERE year='2016'")  


