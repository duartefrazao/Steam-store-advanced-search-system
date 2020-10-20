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

def parse_code(title, code, type):

    prev_org = next((x for x in parsed_organizations if x.code == code), None)


    if  prev_org is None: 
        page = wp.page(wikibase=code, silent=True)
        info = page.get_wikidata(show=False)
        org_title = info.data['title']
        wikipedia_page = wp.page(org_title, skip=['claims', 'imageinfo'])       
        data = wikipedia_page.get_query(show=False).data
        if ('extext' in data):
            text_extract = data['extext']
            new_entries.append([org_title,text_extract.strip('\t\n\r')])
            parsed_organizations.add(Organization(code, org_title))
            df.loc[df['name'] == title, type] = org_title
    else:
        df.loc[df['name'] == title, type] = prev_org.name



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
        query_string = f"""
            PREFIX schema: <http://schema.org/>
            SELECT ?title ?publisher ?developer WHERE {{
                ?item wdt:P31 wd:Q7889 .
                ?item rdfs:label "{title}"@en
                OPTIONAL {{ ?item wdt:P123 ?publisher. }}
                OPTIONAL {{ ?item wdt:P178 ?developer. }}
            }}    
        """

        res = return_sparql_query_results(query_string)

        for row in res["results"]["bindings"]:
                if 'publisher' in row:
                    publisher_code = str(row['publisher']['value']).rsplit('/', 1)[1]
                    parse_code(title, publisher_code, 'publisher')
                if 'developer' in row:
                    developer_code = str(row['developer']['value']).rsplit('/', 1)[1]
                    parse_code(title, developer_code, 'developer')
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

