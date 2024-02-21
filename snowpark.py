#%%
import configparser
import os
from snowflake.snowpark.session import Session
import numpy as np
import pandas as pd
import plotly.express as px
import json
import plotly.graph_objects as go
import datetime
#%%
# creating the session

config = configparser.ConfigParser()
init_path = os.path.join(os.getcwd(),'config.ini')
config.read(init_path)
sfAccount=config['Snowflake']['sfAccount']
sfUser=config['Snowflake']['sfUser']
sfPass=config['Snowflake']['sfPass']
sfRole=config['Snowflake']['sfRole']
sfDatabase=config['Snowflake']['sfDatabase']
sfWarehouse=config['Snowflake']['sfWarehouse']
sfSchema=config['Snowflake']['sfSchema']

CONNECTION_PARAMETERS = {
    "account": sfAccount,
    "user": sfUser,
    "password": sfPass,
    "role": sfRole,
    "authenticator":"username_password_mfa",
    "databaswee": sfDatabase,
    "warehouse": sfWarehouse,
    "schema": sfSchema
}
sessionBuilder = Session.builder

for key, value in CONNECTION_PARAMETERS.items():
    sessionBuilder.config(key, value)

session = sessionBuilder.create()

# %%    Counts the nr of double checkins/checkouts on a single card
# create a pandas dataframe
# cardnr = 2706697231
# df = session.sql(f'''
#     select
#         APPLICATIONTRANSACTIONSEQUENCENUMBER sqnnr,
#         TRANSACTIONTYPE type,
#         BUSINESSDAY date,
#         MEDIASERIALNUMBERID cardid,
#         ROUTEID routeid,
#         STATIONID stationid
#     from TRANSDEV.BSL.TRANSACTIONS_ALL_FIN
#     where MEDIASERIALNUMBERID = '{cardnr}'
# '''     
# ).to_pandas()

# #%%
# curr_sqnnr = min(df["SQNNR"])
# last_type = None 
# last_date = None
# last_route = None 
# double_cico_today = 0
# total_double_cico = 0
# total_ci = 0
# max_sqnnr = max(df["SQNNR"])

# while curr_sqnnr <= max_sqnnr:
#     curr_row = df.loc[df["SQNNR"] == curr_sqnnr]
#     if len(curr_row) == 1:
#         try:
#             curr_type = int(curr_row["TYPE"])
#             curr_date = curr_row['DATE'].values[0]
#             curr_route = curr_row["ROUTEID"].values[0]
#         except:
#             print(f'curr row:\n{curr_row.values[0]}')
#             raise AssertionError
#         if (curr_date == last_date):
#             if (curr_route == last_route) and ((curr_type == 30 and last_type == 31) or (curr_type == 32 and last_type == 33)):
#                 double_cico_today += 1
#             else:
#                 if double_cico_today > 1:
#                     total_double_cico += double_cico_today
#                     double_cico_today = 0
#         if (curr_type == 30) or (curr_type == 32):
#             total_ci += 1
#     else:
#         curr_type, curr_date, curr_route = None, None, None

#     last_type = curr_type
#     last_date = curr_date 
#     last_route = curr_route
#     curr_sqnnr += 1
# print(f'card nr: {cardnr}. \nTotal double CiCo: {total_double_cico} during {len(df)} transactions, of which {total_ci} check-ins\n\n(*does not count single double CiCo on one day)')

# # %%
# print(df.columns)
# print(df.loc[1].values)









#%%
global df
df = session.sql(f'''
    select
        APPLICATIONTRANSACTIONSEQUENCENUMBER sqnnr,
        TRANSACTIONTYPE type,
        BUSINESSDAY date,
        MEDIASERIALNUMBERID cardid,
        ROUTEID routeid,
        STATIONID stationid
    from TRANSDEV.BSL.TRANSACTIONS_ALL_FIN
    where (DATE >= '2023-6-01') and (DATE < '2023-9-01') and (ROUTEID = 5212)
'''     
).to_pandas()
# Lijn 397 heeft routeID 5212
df = df.set_index(keys=["CARDID", "SQNNR"])

# %%
def check_double_cico2(row, prevrow):
    # try:
    ttype, date, routeid, stationid = row.values
    ptype, pdate, prouteid, pstationid = prevrow.values
    transactiontypes = (30, 31, 32, 33)
    return (routeid is prouteid) and (stationid is pstationid) and (date is pdate) and (ttype in transactiontypes) and (ptype in transactiontypes) 
    # except:
    #     print(prevrow.values)
    #     raise AssertionError
df_mask = []
for (cardid, sqnnr), row in df.iterrows():
    try:
        prevrow = df.loc[cardid, sqnnr-1][0]
        if check_double_cico2(row, prevrow):
            df_mask.append(True)
    except KeyError:
        df_mask.append(False)

df_filtered = df[df_mask]
print(df_filtered)  

# %%
def check_double_cico(row):
    try:
        prevrow = df.loc[cardid, sqnnr-1][0]
        ttype, date, routeid, stationid = row.values
        ptype, pdate, prouteid, pstationid = prevrow.values
        transactiontypes = (30, 31, 32, 33, 36, 37, 38, 39)
        return int((routeid is prouteid) and (stationid is pstationid) and (date is pdate) and (ttype in transactiontypes) and (ptype in transactiontypes))
    except KeyError:
        return 0
df['double_cico'] = df.apply(check_double_cico, axis=1)
print(sum(df['double_cico']))
# %%
len(df.loc[df['TYPE'].isin([30, 31, 32, 33, 36, 37, 38, 39])])
