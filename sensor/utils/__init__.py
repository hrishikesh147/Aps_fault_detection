import pandas as pd
from sensor.config import mongo_client
from sensor.logger import logging
from sensor.exception import SensorException
import os,sys
import yaml
import numpy as np 
import dill

def get_collection_as_dataframe(database_name:str,collection_name:str)->pd.DataFrame:
    try:
        logging.info(f"reading data from database {database_name} and collection {collection_name}")
        df=pd.DataFrame(list(mongo_client[database_name][collection_name].find()))

        if "_id" in df.columns:
            logging.info(f"checking if _id is present, rows and columns{df.shape}")
            df.drop("_id",axis=1)
            logging.info(f"dropping _id,new rows and columns {df.shape}")
        return df
    except Exception as e:
        raise SensorException(e,sys)