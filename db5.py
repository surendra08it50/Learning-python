from sqlalchemy import create_engine

#db_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
db_string = "postgresql+psycopg2://lr_pguser:lr_pgpass@dcaldd1016:5432/learning_rate"
db = create_engine(db_string)

# Create 
#db.execute("CREATE TABLE IF NOT EXISTS films (title text, director text, year text)")  
#db.execute("INSERT INTO films (title, director, year) VALUES ('Doctor Strange', 'Scott Derrickson', '2016')")

# Read

result_set = db.execute('SELECT * FROM PUBLIC."OWPS_ERROR_HANDLING_aic_owps_drift_op"')  
for r in result_set:  
    print("------********66666666", r)

# Update
#db.execute("UPDATE films SET title='Some2016Film' WHERE year='2016'")

# Delete
#db.execute("DELETE FROM films WHERE year='2016'")  