import requests
import pandas as pd
import wptools as wp
from time import sleep
from qwikidata.sparql  import return_sparql_query_results

df = pd.read_csv ("aggregation/misses.csv")
session = requests.Session()
new_entries = []
errors = []
parsed_organizations = set()

class Organization(object):
    def __init__(self, code, name):
        self.code = code
        self.name = name
    def __eq__(self,other):
        if isinstance(other, Organization):
            return self.code == other.code
        else:
            return False
    def __hash__(self):
        return hash(self.code)

def parse_code(title, org, type):

    if(org not in parsed_organizations):
        wikipedia_page = wp.page(org.name, skip=['claims', 'imageinfo'])       
        data = wikipedia_page.get_query(show=False).data
        if ('extext' in data):
            text_extract = data['extext']
            new_entries.append([org.name,text_extract.strip('\t\n\r')])
            parsed_organizations.add(org)
    
    df.iloc[df['name'] == title, type] = org.name



def query(title, org, type):
    sleep(1)

    org = org.split(",")[0]
    print("Querying ", org)

    try:
        publisher_query = f"""
            SELECT ?publisher ?publisherLabel WHERE {{
              ?publisher wdt:P31 wd:Q1137109.
              ?publisher rdfs:label ?publisherLabel.
              filter contains(?publisherLabel,"{org}") 
              filter langMatches(lang(?publisherLabel),'en')
            }}
        """
        developer_query = f"""
            SELECT ?developer ?developerLabel WHERE {{
              ?developer wdt:P31 wd:Q210167.
              ?developer rdfs:label ?developerLabel.
              filter contains(?developerLabel,"{org}") 
              filter langMatches(lang(?developerLabel),'en')
            }}
        """
        publishers = return_sparql_query_results(publisher_query)["results"]["bindings"]
        developers = return_sparql_query_results(developer_query)["results"]["bindings"]

        for row in publishers:
                if 'publisherLabel' in row:
                    parse_code(title, Organization(row['publisher']['value'],row['publisherLabel']['value']), type)
        for row in developers:
                if 'developerLabel' in row:
                    parse_code(title, Organization(row['developer']['value'],row['developerLabel']['value']), type)
    except:
        print("Could not query ", title)
        errors.append(title)

def myFunc(entries):
    for entry in entries:
        title = entry[0]
        developer = entry[1]
        publisher = entry[2]
        query(title, developer, 4)
        query(title, publisher, 5)
    # create and save organizations csv
    organization_df = pd.DataFrame(new_entries, columns=['organization', 'description'])
    organization_df.replace(to_replace=[r"\\t", r"\\n|\\r", "\t|\n|\r"], value=["\t","\n","\n"], regex=True, inplace=True)
    organization_df.to_csv('publisher-info-2.csv', index=False)
    # update steam-store csv
    df.to_csv("updated-store.csv")

myFunc(df.iloc[0:1000, [1,4,5]].values)