import requests
import pandas as pd
import wptools as wp
from time import sleep
from qwikidata.sparql  import return_sparql_query_results

df = pd.read_csv ("steam-store.csv")

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
    
    df.loc[df['name'] == title, type] = org.name
    


def query(title):
    sleep(1)
    print("querying: ", title)
    # query_string = f"""
    #     PREFIX schema: <http://schema.org/>
    #     SELECT ?title ?publisher ?publisherLabel $article WHERE {{
    #         ?item wdt:P31 wd:Q7889 .
    #         ?item rdfs:label "{title}"@en
    #         OPTIONAL {{ ?item wdt:P123 ?publisher. }}
    #         OPTIONAL {{ 
    #             ?article schema:about ?publisher .
    #             ?article schema:inLanguage "en" .
    #             ?article schema:isPartOf <https://en.wikipedia.org/> . 
    #         }}
    #     }}    
    # """
    try:
        publisher_query = f"""
            SELECT ?gameLabel ?publisher ?publisherLabel WHERE {{
                hint:Query hint:optimizer "None" .
                
                ?game wdt:P31 wd:Q7889 .
                ?game rdfs:label "{title}"@en .
                ?game wdt:P123 ?publisher .
                
                ?publisherProp wikibase:directClaim wdt:P123 . 

                SERVICE wikibase:label {{bd:serviceParam wikibase:language "en" .}}
                }}
        """
        developer_query = f"""
            SELECT ?gameLabel ?developer ?developerLabel WHERE {{
                hint:Query hint:optimizer "None" .
                
                ?game wdt:P31 wd:Q7889 .
                ?game rdfs:label "{title}"@en .
                ?game wdt:P178 ?developer .
                
                ?developerProp wikibase:directClaim wdt:P178 . 

                SERVICE wikibase:label {{bd:serviceParam wikibase:language "en" .}}
                }}
        """
        # query_string = f"""
        #     PREFIX schema: <http://schema.org/>
        #     SELECT ?title ?publisher ?developer WHERE {{
        #         ?item wdt:P31 wd:Q7889 .
        #         ?item rdfs:label "{title}"@en
        #         OPTIONAL {{ ?item wdt:P123 ?publisher. }}
        #         OPTIONAL {{ ?item wdt:P178 ?developer. }}
        #     }}    
        # """

        publishers = return_sparql_query_results(publisher_query)["results"]["bindings"]
        developers = return_sparql_query_results(developer_query)["results"]["bindings"]

        for row in publishers:
                if 'publisherLabel' in row:
                    parse_code(title, Organization(row['publisher']['value'],row['publisherLabel']['value']), 'publisher')
        for row in developers:
                if 'developerLabel' in row:
                    parse_code(title, Organization(row['developer']['value'],row['developerLabel']['value']), 'developer')
    except:
        print("Could not query ", title)
        errors.append(title)

def myFunc(names):
    for name in names:
        print(name)
        query(name)
    # create and save organizations csv
    organization_df = pd.DataFrame(new_entries, columns=['organization', 'description'])
    organization_df.replace(to_replace=[r"\\t", r"\\n|\\r", "\t|\n|\r"], value=["\t","\n","\n"], regex=True, inplace=True)
    organization_df.to_csv('publisher-info.csv', index=False)
    # update steam-store csv
    df.to_csv("updated-store.csv")
    # write error
    errors_df = pd.DataFrame(errors, columns=['title'])
    errors_df.to_csv('errors.csv')


myFunc(df["name"].values)

