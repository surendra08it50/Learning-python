import urllib.request

user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'

url = "https://www.worldometers.info/gdp/india-gdp"
headers={'User-Agent':user_agent,} 

request=urllib.request.Request(url,None,headers) #The assembled request
response = urllib.request.urlopen(request)
data = response.read() # The data u need

from bs4 import BeautifulSoup
soup = BeautifulSoup(data, 'lxml') # Parse the HTML as a string
    
table = soup.find_all('table')[1] # Grab the first table

new_table = pd.DataFrame() # I know the size 
    
row_marker = 0
for row in table.find_all('tr'):
    column_marker = 0
    columns = row.find_all('td')
    for column in columns:
        new_table.at[row_marker,column_marker] = column.get_text()
        column_marker += 1
    row_marker =row_marker+1
    
new_table

new_table.to_csv("gdp.csv")