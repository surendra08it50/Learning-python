from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/surendra', echo = True)
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class Customers(Base):
   __tablename__ = 'customers'
   
   id = Column(Integer, primary_key = True)
   name = Column(String)

   address = Column(String)
   email = Column(String)

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind = engine)
session = Session()
result = session.query(Customers).filter(Customers.id>2)

for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)
