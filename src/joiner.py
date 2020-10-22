import requests
import pandas as pd
import wptools as wp
import glob, os

store_df = pd.read_csv("aggregation/store/final-store.csv")

# store_ang = pd.read_csv("aggregation/store/updated-store-ang.csv", nrows=6749).iloc[:, 1:]

# store_dua = pd.read_csv("aggregation/store/updated-store-ang.csv").iloc[6749:13499, 1:]

# store_ped = pd.read_csv("aggregation/store/updated-store-ped.csv").iloc[13499:, 1:]

fixed_misses = pd.read_csv("updated-store.csv").iloc[:15000]

# store_df = store_ang.append(store_dua, ignore_index=True)
# store_df = store_df.append(store_ped, ignore_index=True)

for row in fixed_misses.itertuples(index=False, name='Pandas'):
    store_df.loc[store_df['name'] == row.name,'developer'] = row.developer
    store_df.loc[store_df['name'] == row.name,'publisher'] = row.publisher
    

store_df.to_csv("aggregation/store/final-store.csv",  index=False)

organizations_df = pd.concat(map(pd.read_csv, glob.glob(os.path.join('aggregation/organizations/', "*.csv"))))

organizations_df = organizations_df.drop_duplicates()

print(organizations_df.nunique())

organizations_df.to_csv("aggregation/organizations/final.csv", index=False)

misses = []

for row in store_df.itertuples(index=False, name='Pandas'):
    if not ((organizations_df['organization'] == row.developer).any() or (organizations_df['organization'] == row.publisher).any()):
        misses.append(row)

missed_df = pd.DataFrame(misses)
missed_df.to_csv("aggregation/misses.csv", index=False)

print("Missing percentage: ", len(missed_df.values) / len(store_df.values))