import csv
import json
from distutils.util import strtobool

def csv_to_json(csvFilePath, jsonFilePath):
    jsonArray = []

    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        for row in csvReader:
            jsonArray.append(row)
    
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)

def fix_games(jsonFilePath):
    with open(jsonFilePath) as data_file:
        data = json.load(data_file)
        for game in data:
            # split categories, genres and platforms into arrays
            categories = game['categories'].split(";")
            game['categories'] = categories
            genres = game['genres'].split(";")
            game['genres'] = genres
            platforms = game['platforms'].split(";")
            game['platforms'] = platforms

            # convert numeric values from string to int
            game['appid'] = int(game['appid'])
            game['required_age'] = int(game['required_age'])
            game['positive_ratings'] = int(game['positive_ratings'])
            game['negative_ratings'] = int(game['negative_ratings'])
            game['average_playtime'] = int(game['average_playtime'])
            game['median_playtime'] = int(game['median_playtime'])
            game['owners'] = int(game['owners'])
            game['price'] = float(game['price'])

            # convert boolean types
            game['is_english'] = bool(strtobool(game['is_english']))

            # add reviews field
            game['reviews'] = []

    with open(jsonGamesFilePath, 'w', encoding='utf-8') as jsonf:
        jsonString = json.dumps(data, indent=4)
        jsonf.write(jsonString)

def fix_reviews(jsonFilePath):
    with open(jsonFilePath) as data_file:
        data = json.load(data_file)
        for review in data:
            #convert numeric values from string to int
            review['appid'] = int(review['appid'])
            review['sentiment'] = int(review['sentiment'])
            review['number_helpful'] = int(review['number_helpful'])
        
    with open(jsonReviewsFilePath, 'w', encoding='utf-8') as jsonf:
        jsonString = json.dumps(data, indent=4)
        jsonf.write(jsonString)
        

csvGamesFilePath = r'steam_store.csv'
csvReviewsFilePath = r'steam_reviews.csv'
jsonGamesFilePath = r'steam_store.json'
jsonReviewsFilePath = r'steam_reviews.json'
#csv_to_json(csvGamesFilePath, jsonGamesFilePath)
csv_to_json(csvReviewsFilePath, jsonReviewsFilePath)
#fix_games(jsonGamesFilePath)
#fix_reviews(jsonReviewsFilePath)
