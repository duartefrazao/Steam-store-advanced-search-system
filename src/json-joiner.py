import csv
import json

def addReviewsAttributeToGames():
  for game in games:
    game["reviews"] = []

def addReviewsToGame(currentGame, currentGameReviews):
  for game in games:
    if(game['appid'] == currentGame):
      game['reviews']=currentGameReviews

def iterateReviews():
  currentGame = reviews[0]['appid']
  currentGameReviews = []
  for review in reviews:
    if(currentGame != review['appid']):
      addReviewsToGame(currentGame,currentGameReviews)
      currentGame = review['appid']
      currentGameReviews = []
    
    currentGameReviews.append(review)

  addReviewsToGame(currentGame,currentGameReviews)

  

with open('steam_reviews.json') as reviewsFile:
  reviews = json.load(reviewsFile)

with open('steam_store.json') as gamesFile:
  games = json.load(gamesFile)

addReviewsAttributeToGames()
iterateReviews()

jsonFinalPath = r'games_with_reviews.json'
with open(jsonFinalPath, 'w', encoding='utf-8') as jsonf:
        jsonString = json.dumps(games, indent=4)
        jsonf.write(jsonString)