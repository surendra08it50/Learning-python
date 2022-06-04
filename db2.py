from sqlalchemy import create_engine

#db_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
db_string = "postgresql+psycopg2://postgres:admin@localhost:5432/surendra"
db = create_engine(db_string)

# Create 
db.execute("CREATE TABLE IF NOT EXISTS films (title text, director text, year text)")  
db.execute("INSERT INTO films (title, director, year) VALUES ('Doctor Strange', 'Scott Derrickson', '2016')")

# Read
result_set = db.execute("SELECT * FROM films")  
for r in result_set:  
    print(r)

# Update
#db.execute("UPDATE films SET title='Some2016Film' WHERE year='2016'")

# Delete
#db.execute("DELETE FROM films WHERE year='2016'")  


