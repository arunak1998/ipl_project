import sys
import os

from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    batsman_stat_data_path:str=os.path.join('artifacts','individual_batsman.csv')
    batsman_opp_stat_data_path:str=os.path.join('artifacts','batsman_opposite_runs.csv')
    batsman_opp_innings_stat_data_path:str=os.path.join('artifacts','batsman_opposite_innings_runs.csv')
    batsman_opp_venue_stat_data_path:str=os.path.join('artifacts','batsman_opposite_venue_runs.csv')
    batsman_opp_venue_inning_stat_data_path:str=os.path.join('artifacts','batsman_opposite_venue_inning__runs.csv')
    batsman_venue_stat_data_path:str=os.path.join('artifacts','batsman_venue_runs.csv')
    batsman_venue_inning_stat_data_path:str=os.path.join('artifacts','batsman_venue_inning__runs.csv')