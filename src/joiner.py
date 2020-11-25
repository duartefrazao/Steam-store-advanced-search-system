import requests
import pandas as pd
import wptools as wp
import glob, os

# store_df = pd.read_csv("aggregation/node/store/final-store.csv")

store_5 = pd.read_csv("aggregation/node/store/node-updated-store-5000.csv").iloc[:5000, 1:]

store_10 = pd.read_csv("aggregation/node/store/node-updated-store-10000.csv").iloc[5000:15000, 1:]

store_15 = pd.read_csv("aggregation/node/store/node-updated-store-15000.csv").iloc[15000:20000, 1:]

store_20 = pd.read_csv("aggregation/node/store/node-updated-store-20000.csv").iloc[20000:25000, 1:]

store_25 = pd.read_csv("aggregation/node/store/node-updated-store-25000.csv").iloc[25000:, 1:]

# store_27 = pd.read_csv("aggregation/node/store/node-updated-store-27000.csv").iloc[27000:, 1:]


store_df = store_5.append(store_10, ignore_index=True)
store_df = store_df.append(store_15, ignore_index=True)
store_df = store_df.append(store_20, ignore_index=True)
store_df = store_df.append(store_25, ignore_index=True)
# store_df = store_df.append(store_27, ignore_index=True)
# store_df = store_df.append(store_ped, ignore_index=True)

# for row in fixed_misses.itertuples(index=False, name='Pandas'):
#     store_df.loc[store_df['name'] == row.name,'developer'] = row.developer
#     store_df.loc[store_df['name'] == row.name,'publisher'] = row.publisher
    


organizations_df = pd.concat(map(pd.read_csv, glob.glob(os.path.join('aggregation/node/orgs/', "*.csv"))))

organizations_df['organization'] = organizations_df['organization'].str.replace('_', ' ')
organizations_df['organization'] = organizations_df['organization'].str.replace(r'.*\(.*\)', '')
organizations_df['organization'] = organizations_df['organization'].str.replace('"', '')


organizations_df = organizations_df[~(organizations_df['description'].str.contains("refer to"))]

organizations_df = organizations_df.drop_duplicates(subset='organization')

organizations_df = organizations_df.drop_duplicates(subset='description')

store_df.to_csv("aggregation/node/store/final-store.csv",  index=False)

store_df['developer'].to_csv('dev_names.csv')
store_df['publisher'].to_csv('publisher_names.csv')

print(organizations_df.nunique())

organizations_df['organization'].to_csv("org_names.csv")

organizations_df.to_csv("aggregation/node/final.csv", index=False)

misses = []

for row in store_df.itertuples(index=False, name='Pandas'):
    if not ((organizations_df['organization'] == row.developer).any() or (organizations_df['organization'] == row.publisher).any()):
        misses.append(row)

missed_df = pd.DataFrame(misses)
# missed_df.to_csv("aggregation/misses.csv", index=False)

print("Missing percentage: ", len(missed_df.values) / len(store_df.values))