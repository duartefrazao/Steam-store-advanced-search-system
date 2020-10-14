import requests
import pandas as pd
from qwikidata.sparql  import return_sparql_query_results

df = pd.read_csv ("test.csv")

def query(title):

    session = requests.Session()

    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "prop": "pageprops",
        "titles": title,
    }

    response = session.get(url=url, params=params)
    data = response.json()
    pages = data["query"]["pages"]

    for key, val in pages.items():
        query_string = f"""
            PREFIX schema: <http://schema.org/>
            SELECT ?title ?publisher ?publisherLabel $article WHERE {{
                wd:{val['pageprops']['wikibase_item']} wdt:P123 ?publisher.
                OPTIONAL {{ wd:{val['pageprops']['wikibase_item']} wdt:P1476 ?title. }}
                OPTIONAL {{ 
                    ?article schema:about ?publisher .
                    ?article schema:inLanguage "en" .
                    ?article schema:isPartOf <https://en.wikipedia.org/> . 
                }}
            }}    
        """

        res = return_sparql_query_results(query_string)

        for row in res["results"]["bindings"]:
            print("Publisher:", row['article']['value'][30:])

    

def myFunc(names):
    print(names)
    for name in names:
        query(name)


myFunc(df["name"].values)

