import os
import sys
import numpy as np
import pandas as pd
from src.logger import logging


### Convert the Data from mongo db to datframe to store in CSV 
def flatten(data1):
    data=[]
    for doc in data1:
    
        
       
        
        # Iterate over each key-value pair in the document
        for team, stats in doc.items():
            flattened_data = {}
            # Skip keys that are MongoDB document metadata (e.g., "_id")
            if team != "_id":
                # Iterate over each stat in the stats dictionary
                for stat, value in stats.items():
                    if isinstance(value, (list, dict)):
                        continue
        
                    flattened_data["_id"] = doc["_id"]
                    # Store the team name as "team_played" column
                    flattened_data["team_played"] = team
                    # Store the stat name and value as columns in flattened_data
                    flattened_data[stat] = value
                data.append(flattened_data)    
             
    return data

### Convert the Data from mongo db of Multiple array field to datframe to store in CSV 
def flattenarray(data1):
    data=[]
    for doc in data1:
    
        # Iterate over each key-value pair in the document
        for team, stats in doc.items():
            flattened_data = {}
            # Skip keys that are MongoDB document metadata (e.g., "_id")
            if team != "_id":
                # Iterate over each stat in the stats dictionary
                for stat, value in stats.items():
                    if isinstance(value, (list, dict, np.ndarray)):
                        for item in value:
                              flattened_data = {}
                              for stat2,value2 in item.items():
                                 for stat3,value3 in value2.items():
                        
        
                                    flattened_data["_id"] = doc["_id"]
                                    # Store the team name as "team_played" column
                                    flattened_data["team_played"] = team
                                    flattened_data["innings_type"] =stat2
                                    # Store the stat name and value as columns in flattened_data
                                    flattened_data[stat3] = value3
                              data.append(flattened_data)    
             
    return data


### Convert the Data from mongo db of Multiple array field as object to datframe to store in CSV 
def flattenarrayforvenue(data1):
    data=[]
    for doc in data1:
   
        # Iterate over each key-value pair in the document
        for team, stats in doc.items():
            flattened_data = {}
            # Skip keys that are MongoDB document metadata (e.g., "_id")
            if team != "_id":
                # Iterate over each stat in the stats dictionary
                    
                        for stat, value in stats.items():
                              flattened_data = {}
                             
                                 
                              for stat2,value2 in value.items():
                                 
                        
                                    if isinstance(value2, (list, dict)):
                                        continue
                                    flattened_data["_id"] = doc["_id"]
                                    # Store the team name as "team_played" column
                                    flattened_data["team_played"] = team
                                    flattened_data["venues_played"] =stat
                                    # Store the stat name and value as columns in flattened_data
                                    flattened_data[stat2] = value2
                              data.append(flattened_data)    
             
    return data


### Convert the Data from mongo db of triple array field as object to datframe to store in CSV 
def flattenarrayforvenueinnings(data1):
    data=[]
    for doc in data1:
   
        # Iterate over each key-value pair in the document
        for team, stats in doc.items():
            flattened_data = {}
            # Skip keys that are MongoDB document metadata (e.g., "_id")
            if team != "_id":
                # Iterate over each stat in the stats dictionary
                
                        for stat, value in stats.items():
                              flattened_data = {}
                                
                              for stat2,value2 in value.items():
                                 
                        
                                     if isinstance(value2, (list, dict, np.ndarray)):
                                        for item in value2:
                                            flattened_data = {}
                                            for stat4,value5 in item.items():
                                                for stat3,value3 in value5.items():
                                                    flattened_data["_id"] = doc["_id"]
                                                    # Store the team name as "team_played" column
                                                    flattened_data["team_played"] = team
                                                    flattened_data["venues_played"] =stat
                                                    flattened_data["innings"] =stat4
                                                    # Store the stat name and value as columns in flattened_data
                                                    flattened_data[stat3] = value3
                                            data.append(flattened_data)    
             
    return data



def cursortodataframe(stat,type):
    
    '''
   
      The `type` parameter is used to identify which flattening method to call for converting the cursor to a DataFrame.
   
    '''
    cursor = stat.find()

    if type is not None and type =='single':
         data = flatten(cursor)

    if type is not None and type =='multiple':
         data = flattenarray(cursor)
    if type is not None and type =='triple':
         data = flattenarrayforvenue(cursor)
    if type is not None and type =='four':
         data = flattenarrayforvenueinnings(cursor)

    # Convert cursor to list of dictionaries and flatten nested objects
    



    # Create DataFrame
    df = pd.DataFrame(data)
    
    return df