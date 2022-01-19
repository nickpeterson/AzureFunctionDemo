import azure.functions as func
from typing import List
import logging
import os
import json
import logging
import requests
import msal
import pyodbc
import uuid
import sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

# 1. Loop through each incoming message and determine if it is an Add User or Delete User Operation
# 2. If it is try grabbing a API token from cache using MSAL if one doesnt exist generate a new one
# 3. Make an api call to grab the information we need about the user who was either just created or deleted
# 4. Call the appropriate function to either add the new user to LDW or remove them

# Written by nick.peterson@luckcompanies.com


def main(events: List[func.EventHubEvent]):

    ##logging.info("Function has been Triggered")

    for event in events:

        message_content = json.loads(event.get_body().decode('utf-8'))
        # logging.info(message_content)
        record = message_content['records']

        for i in range(len(record)):

            # Variables to be used throughout main Loop
            userID = ""
            userAdded = False
            userDeleted = False

            if record[i]["operationName"] == 'Add user':
                userID = record[i]['properties']['targetResources'][i]['id']
                userAdded = True
                logging.info(f'New user has been added: {userID}')
            elif record[i]["operationName"] == 'Delete user':
                userID = record[i]['properties']['targetResources'][i]['id']
                userDeleted = True

            if userAdded or userDeleted:

                params = open('AuditLogsEventHubTrigger\parameters.json', "r")
                config = json.loads(params.read())

                app = msal.ConfidentialClientApplication(
                    config["client_id"], authority=config["authority"],
                    client_credential=config["secret"])

                result = None
                result = app.acquire_token_silent(
                    config["scope"], account=None)

                if not result:
                    logging.info(
                        "No suitable token exists in cache. Let's get a new one from AAD.")
                    result = app.acquire_token_for_client(
                        scopes=os.environ["scope"])

                graph_data = requests.get(  # Use token to call downstream service
                    f'https://graph.microsoft.com/v1.0/users/{userID}?$select=userPrincipalName,displayName,mail,onPremisesDistinguishedName,extension_1aa69b795e904d03b8aba14158d40168_division,extension_1aa69b795e904d03b8aba14158d40168_department',
                    headers={'Authorization': 'Bearer ' + result['access_token']}, ).json()

                logging.info(json.dumps(graph_data, indent=2))

                userPrincipal = graph_data['userPrincipalName']
                displayName = graph_data['displayName']
                mail = graph_data['mail']
                distinguisedName = graph_data['onPremisesDistinguishedName']
                department = graph_data['extension_1aa69b795e904d03b8aba14158d40168_department']
                division = graph_data['extension_1aa69b795e904d03b8aba14158d40168_division']

                adlsJson = {}
                adlsJson['user'] = []
                adlsJson['user'].append({
                    'userPrincipal': f'{userPrincipal}',
                    'displayName': f'{displayName}',
                    'mail': f'{mail}',
                    'distinguishedName': f'{distinguisedName}',
                    'department': f'{department}',
                    'division': f'{division}'


                })

                if userAdded:
                    createNewUser(userPrincipal, displayName, department,
                                  division, distinguisedName, mail)
                  #  writeFiletoADLS(adlsJson)

                elif userDeleted:
                    removeUser(userPrincipal)


def createNewUser(userPrincipalName, displayName, department, division, distinguisedName, mail):
    server = 'luckanalyticssql-dev.database.windows.net'
    database = 'LuckDataWarehouse_Dev'
    username = 'LuckSourceDataMover'
    password = os.getenv('PasswordFromKV')
    driver = '{ODBC Driver 17 for SQL Server}'

    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD=' + password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO test.ADSync (userPrincipalName, displayName, department, division, DistinguishedName, Mail) VALUES ('{userPrincipalName}','{displayName}','{department}','{division}','{distinguisedName}','{mail}')")
            cursor.commit()
            logging.info(f"New User {userPrincipalName} has Been Added")


def removeUser(userPrincipalName):
    server = 'luckanalyticssql-dev.database.windows.net'
    database = 'LuckDataWarehouse_Dev'
    username = 'LuckSourceDataMover'
    password = os.getenv('PasswordFromKV')
    driver = '{ODBC Driver 17 for SQL Server}'

    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD=' + password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"delete from test.ADSync where userPrincipalName = '{userPrincipalName}'")
            cursor.commit()
            logging.info(f"User {userPrincipalName} has Been Removed")


# def writeFiletoADLS(file):
#     storage_account_name = "luckdatalakeprod"
#     storage_account_key = "jDmE04P2vP74G6wN6EZ3h6y6ucNEuAH1FfKlsgF+EliMQO4kZI110itsQpjqWlOMmiBeA5tCdfSSFbf1IJulmw=="

#     try:
#         global service_client

#         service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
#             "https", storage_account_name), credential=storage_account_key)

#     except Exception as e:
#         print(e)

#     try:

#         file_system_client = service_client.get_file_system_client(
#             file_system="azure-active-directory")

#         directory_client = file_system_client.get_directory_client("users")

#         file_client = directory_client.get_file_client("nick.json")
#         filesize_previous = file_client.get_file_properties().size

#         file_to_load = json.dumps(file)

#         file_client.append_data(
#             data=file_to_load, offset=filesize_previous, length=len(file_to_load))

#         file_client.flush_data(len(file_to_load) + filesize_previous)

#         logging.info("File Successfully Written")

#     except Exception as e:
#         print(e)
