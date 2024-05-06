import sys
import os

from src.exception import Customexception
from src.config.connection import mongoconnect
from src.logger import logging
from src.utils import  cursortodataframe
from dataclasses import dataclass
from src.config.dataingestionconfig import DataIngestionConfig

class DataInjestion:
    def __init__(self):
        self.injestion_config=DataIngestionConfig()

    def initiate_dataingestion(self):
        logging.info("Data Injestion process from Mongo DB Started")

        try:
            df_opp_stat=cursortodataframe(mongoconnect("batsman_opposite_team_stat"),"single")
            os.makedirs(os.path.dirname(self.injestion_config.batsman_opp_stat_data_path),exist_ok=True)
            df_opp_stat.to_csv(self.injestion_config.batsman_opp_stat_data_path,index=False,header=True)

            df_opp_inning_stat=cursortodataframe(mongoconnect("batsman_opposite_team_stat"),"multiple")
            os.makedirs(os.path.dirname(self.injestion_config.batsman_opp_innings_stat_data_path),exist_ok=True)
            df_opp_inning_stat.to_csv(self.injestion_config.batsman_opp_innings_stat_data_path,index=False,header=True)

            df_venue_stat=cursortodataframe(mongoconnect("batsman_venue_stat"),"single")
            os.makedirs(os.path.dirname(self.injestion_config.batsman_venue_stat_data_path),exist_ok=True)
            df_venue_stat.to_csv(self.injestion_config.batsman_venue_stat_data_path,index=False,header=True)

            df_venue_inning_stat=cursortodataframe(mongoconnect("batsman_venue_stat"),"multiple")
            os.makedirs(os.path.dirname(self.injestion_config.batsman_venue_inning_stat_data_path),exist_ok=True)
            df_venue_inning_stat.to_csv(self.injestion_config.batsman_venue_inning_stat_data_path,index=False,header=True)

            df_venue_opp_stat=cursortodataframe(mongoconnect("batsman_opposite_team_venue_stat"),"triple")
            os.makedirs(os.path.dirname(self.injestion_config.batsman_opp_venue_stat_data_path),exist_ok=True)
            df_venue_opp_stat.to_csv(self.injestion_config.batsman_opp_venue_stat_data_path,index=False,header=True)


            df_venue_opp_inning_stat=cursortodataframe(mongoconnect("batsman_opposite_team_venue_stat"),"four")
            os.makedirs(os.path.dirname(self.injestion_config.batsman_opp_venue_inning_stat_data_path),exist_ok=True)
            df_venue_opp_inning_stat.to_csv(self.injestion_config.batsman_opp_venue_inning_stat_data_path,index=False,header=True)


            logging.info("Data Injestion process from Mongo DB completed")

        except Exception as e:
            raise Customexception(e,sys)




if __name__=='__main__':

    a=DataInjestion()
    a.initiate_dataingestion()
