An advanced search system that gathers information from several sources and provides two types of search.
The first is a data retrieval system based on Solr, where several steps were made do accelerate and improve the quality of search results: indexing, filtering, synonym matching and stemming.
The second is a web ontology created with Protégé that was populated with the same information and that can be queried with SPARQL.


### Sources
- Wikipedia/Wikidata API for game and publisher/developer information 
- Steam datasets related to games (information, requirements, description, reviews)


### Examples


## Instructions to use solr

1. Copy steamgames folder to <your solr folder>/server/solr/configsets
1. In your solr folder, run ./bin/solr start -e cloud and select the defaults. When it asks for a collection name and a configset, write steamgames on both.
1. You should have running on localhost:8983 solr with the created collection and correct schema.

You only need to do this the first them. On subsequent runs, you can simply run `./bin/solr start -e cloud -noprompt`
To stop solr, run `./bin/solr stop -all` 

You can index data with this command (make sure you are in solr folder):
```
./bin/post -c steamgames -params "f.categories.split=true&f.categories.separator=%3B&f.genres.split=true&f.genres.separator=%3B&f.platforms.split=true&f.platforms.separator=%3B" -type text/csv <path-to-your-csv-file>
```

