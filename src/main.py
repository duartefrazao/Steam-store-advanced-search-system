import requests
import pandas as pd
import wptools as wp
from time import sleep
from qwikidata.sparql  import return_sparql_query_results

df = pd.read_csv ("steam-store.csv")

session = requests.Session()

new_entries = []
parsed_organizations = set()

def query(title):

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
    query_string = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?title ?publisher WHERE {{
            ?item wdt:P31 wd:Q7889 .
            ?item rdfs:label "{title}"@en
            OPTIONAL {{ ?item wdt:P123 ?publisher. }}
        }}    
    """

    res = return_sparql_query_results(query_string)

    for row in res["results"]["bindings"]:
        entity = str(row['publisher']['value']).rsplit('/', 1)[1]
        if entity not in parsed_organizations:
            print("Publisher:", entity)
            page = wp.page(wikibase=entity, silent=True)
            info = page.get_wikidata(show=False)
            wikipedia_page = wp.page(info.data['title'])
            text_extract = wikipedia_page.get_query(show=False).data['extext']
            new_entries.append([info.data['title'],text_extract.strip('\t\n\r')])
            parsed_organizations.add(entity)
        else:
            print("Already in list")
            sleep(0.5)

        

def myFunc(names):
    for name in names:
        print(name)
        query(name)
    df = pd.DataFrame(new_entries, columns=['organization', 'description'])
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
    print(df)
    df.to_csv('publisher-info.csv')


myFunc(df["name"].values)

