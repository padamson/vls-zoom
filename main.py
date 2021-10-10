# %%
import os
from typing import List
from datetime import datetime

import pandas as pd
import numpy as np
from pandas import DataFrame
from requests import Response
from decouple import config

from zoom import Zoom

# %
# Before running script, download MemberLists and update next block.
# Link for MemberLists: https://ecommerce.aiche.org/LSDivForumMemberList/login.aspx?_ga=2.111922436.1284516016.1607959991-608215435.1592855586
# (note: you may need to clear your browser's cache; username/password in settings.ini)

# %%
# Items to change
paying_member_list_filename = "MemberListReport_paying_20210909.csv"
delinquent_member_list_filename = "MemberListReport_delinquent_20210909.csv"
primary_datetime_UTC = '2021-08-26T00:00:00Z'
primary_plus_one_datetime_UTC = '2021-08-27T00:00:00Z'


# %%
ZOOM_API_KEY = config("ZOOM_API_KEY")
ZOOM_API_SECRET = config("ZOOM_API_SECRET")
ZOOM_MEETING_ID = config("ZOOM_MEETING_ID")

data_dir = 'data'

zoom = Zoom(ZOOM_API_KEY, ZOOM_API_SECRET, ZOOM_USER_ID)

jwt_token: bytes = zoom.generate_jwt_token()

# %%
datetime_window = [datetime.strptime(primary_datetime_UTC,
                                     zoom.datetime_format_string),
                   datetime.strptime(primary_plus_one_datetime_UTC,
                                     zoom.datetime_format_string)]
list_of_meetings = zoom.get_meeting_ids(datetime_window, jwt_token)

# %%
list_of_meetings
# %%
leadership = pd.read_csv("leadership.csv")

# %%
leadership.email

# %%
def use_email(custom_questions):
    if "'value': 'No'" in str(custom_questions):
        return 'False'
    if "'value': 'Yes'" in str(custom_questions):
        return 'True'
    else:
        return 'Already'


# %%
for meeting in list_of_meetings:
    response: Response = zoom.get_meeting_participants(meeting['id'], jwt_token)
    meeting['participants'] = response.json().get("participants")

    while token := response.json().get("next_page_token"):
        response = zoom.get_meeting_participants(meeting['id'], jwt_token, token)
        meeting['participants'] += response.json().get("participants")

    meeting['participants'] = pd.DataFrame(meeting['participants']).drop(columns=["attentiveness_score"])

    df = meeting['participants']

    df['user_email'] = df['user_email'].str.lower()
    df.join_time = pd.to_datetime(df.join_time).dt.tz_convert("US/Eastern")
    df.leave_time = pd.to_datetime(df.leave_time).dt.tz_convert("US/Eastern")
    df.sort_values(["id", "name", "join_time"], inplace=True)
    df = df.groupby(["user_email"]) \
         .agg({"duration": ["sum"], "join_time": ["min"], "leave_time": ["max"]}) \
         .reset_index() \
         .rename(columns={"duration": "total_duration"})
    df.columns = df.columns.get_level_values(0)
    df.total_duration = round(df.total_duration / 3600, 2)
    df.join_time = df.join_time.dt.strftime("%Y-%m-%d %H:%M:%S")
    df.leave_time = df.leave_time.dt.strftime("%Y-%m-%d %H:%M:%S")
    df['excom'] = df.user_email.isin(leadership.email)
     
    meeting_name = meeting['topic']

    attended = len(df)
    attended_non_excom = len(df) - df.excom.sum()
    stayed = len(df.query('total_duration >= .75'))
    stayed_non_excom = len(df.query('excom == False & total_duration >= .75'))

    print(f"""Meeting name:          {meeting_name}\n"""
          f"""Attended:              {attended}\n"""
          f"""Attended (non-ExComm): {attended_non_excom}\n"""
          f"""Stayed:                {stayed}\n"""
          f"""Stayed (non-ExComm):   {stayed_non_excom}\n""")
     
    meeting['participants'] = df

    for s in ["'", "(", ")"]:
        meeting['topic'] = meeting['topic'].replace(s,'')
    meeting['filename'] = f"{data_dir}/{'_'.join(meeting['topic'].split())}_{meeting['start_time'].split('T')[0]}.csv"
    meeting['participants'].to_csv(meeting['filename'], index=False)


# %%
# get registrants
for meeting in list_of_meetings:
    response: Response = zoom.get_meeting_registrants(meeting['id'], jwt_token)
    meeting['registrants'] = response.json().get("registrants")
    while token := response.json().get("next_page_token"):
        response = zoom.get_meeting_registrants(meeting['id'], jwt_token, token)
        meeting['registrants'] += response.json().get("registrants")
    meeting['registrants'] = pd.DataFrame(meeting['registrants'])

    df = meeting['registrants']
    df['email'] = df['email'].str.lower()
    df['excom'] = df.email.isin(leadership.email)
    df['email_use'] = df.apply(
        lambda x: use_email(x['custom_questions']), axis=1)

    registered = len(df)
    registered_non_excom = len(df) - df.excom.sum()

    print(f"""Meeting name:          {meeting['topic']}\n"""
          f"""Registered:              {registered}\n"""
          f"""Registered (non-ExComm): {registered_non_excom}\n"""
          f"""Already have email:      {sum(df['email_use'] == 'Already')}\n"""
          f"""Use email:               {sum(df['email_use'] == 'True')}\n"""
          f"""Do not use email:        {sum(df['email_use'] == 'False')}\n""")

    meeting['registrants'] = df

    filename = f"{data_dir}/registrants_{'_'.join(meeting['topic'].split())}_{meeting['start_time'].split('T')[0]}.csv"
    columns_to_write = ['first_name','last_name','email','excom','email_use']
    meeting['registrants'].to_csv(filename, index=False, columns=columns_to_write)


# %%
def get_member_list(data_dir, filename):
    members = pd.read_csv(f'{data_dir}/{filename}')
    return members

def member_list_email_to_lowercase(data_dir, filename):
    member_list = get_member_list(data_dir, filename)
    member_list['Email'] = member_list['Email'].str.lower()
    member_list.to_csv(f'{data_dir}/Processed{filename}', index=False)

# %%
def raffle_winner(list_of_meetings, members):
    import random

    eligibles = pd.DataFrame([])
    for meeting in list_of_meetings:
        df = meeting['participants']
        eligibles = eligibles.append(df.loc[(df['total_duration'] >= 0.75) & (df['excom'] == False)], ignore_index=True)

    winner_is_a_member = False

    while winner_is_a_member is False:
        winner_index = random.randint(0, len(eligibles))
        winner = eligibles.iloc[winner_index]
        winner_email = winner['user_email']
        if members['Email'].str.contains(winner_email).sum() > 0:
            winner_is_a_member = True
            winner = members.loc[members['Email'] == winner_email]

    print("Found a raffle winner:")
    return winner

# %%
member_list_email_to_lowercase(data_dir, paying_member_list_filename)
member_list_email_to_lowercase(data_dir, delinquent_member_list_filename)

# %%
members = get_member_list(data_dir, paying_member_list_filename)
winner = raffle_winner(list_of_meetings, members)
for (columnName, columnData) in winner.iteritems():
   print(f'{columnName}: {columnData.values[0]}')

# %%
df = list_of_meetings[0]['participants']
len(df.loc[(df['total_duration'] >= 0.75) & (df['excom'] == False)])
df.iloc[20]

