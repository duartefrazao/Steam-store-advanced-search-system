# How to index games + reviews

## Collection without filters

1. Start Solr `bin/solr start`
2. Create a collection `bin/solr create -c games`
3. Stop Solr `bin/solr stop`
4. Change the `managedschema` file
    * Copy file `no_filters/managedschema` to solr's folder `server/solr/games/conf`
5. Restart Solr `bin/solr start`
6. Index data `bin/post -c games -format solr <file_path>`

## Collection with filters

1. Repeat the previous section with:
    * Another collection name `games_filters`
    * Another schema file `with_filters/managedschema`

## TODO
1. Add stopwords file
2. Add synonyms file