import os
import sys
from src.exception import Customexception
import pymongo
from src.logger import logging


def mongoconnect(collection_name):

    try:

        client=pymongo.MongoClient('mongodb://localhost:27017/')

        my_db=client['Ml_project']

       
        if collection_name == 'batsman_opposite_team_stat':
            batsman_opposite_team_stat=my_db.batsman_opposite_team_stat
            return batsman_opposite_team_stat
        elif collection_name == 'batsman_venue_stat':
            batsman_venue_stat=my_db.batsman_venue_stat
            return batsman_venue_stat
        
       
        elif collection_name == 'batsman_opposite_team_venue_stat':
            batsman_opposite_team_venue_stat=my_db.batsman_opposite_team_venue_stat
            return batsman_opposite_team_venue_stat
        else:
            raise ValueError("Invalid collection name")
        

    
       
        
        

    except Exception as e:
        raise Customexception(e,sys)
 
