from sqlalchemy import create_engine  
from sqlalchemy import Column, String  
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String

db_string = "postgresql+psycopg2://postgres:admin@localhost:5432/surendra"

engine = create_engine(db_string)  
base = declarative_base()

class Film2(base):  
    __tablename__ = 'films2'

    #id = Column(Integer, primary_key=True)
    title = Column(String, primary_key=True)
    director = Column(String)
    year = Column(String)

Session = sessionmaker(engine)  
session = Session()

base.metadata.create_all(engine)

# Create 
doctor_strange = Film2(title="Doctor Strange", director="Scott Derrickson", year="2016")  
session.add(doctor_strange)  
session.commit()

# Read
films = session.query(Film2)  
for film in films:  
    print(film.title)

# Update
# doctor_strange.title = "Some2016Film"  
# session.commit()

# Delete
# session.delete(doctor_strange)  
# session.commit()  
