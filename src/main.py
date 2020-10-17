import requests
import pandas as pd
from qwikidata.sparql  import return_sparql_query_results

df = pd.read_csv ("test.csv")

session = requests.Session()

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
    print(res)

    for row in res["results"]["bindings"]:
        # print("Publisher:", row['article']['value'][30:])
        print("Publisher:", row['publisher'])

    

def myFunc(names):
    for name in names:
        print(name)
        query(name)


myFunc(df["name"].values)

