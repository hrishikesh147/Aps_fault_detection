from sensor.entity import config_entity
from sensor.entity import artifact_entity
from sensor import utils
from sklearn.model_selection import train_test_split
import pandas as pd 
import numpy as np

class DataIngestion:

    def __init__(self,data_ingestion_config:config_entity.DataIngestionConfig):
        self.data_ingestion_config=data_ingestion_config

    def initiate_data_ingestion(self)->artifact_entity.DataIngestionArtifact:
        df:pd.DataFrame = utils.get_collection_as_dataframe(
            database_name=self.data_ingestion_config.database_name,
            collection_name=self.data_ingestion_config.collection_name)

        df.replace(to_replace="na",value=np.NAN,inplace=True)

        os.path.dirname(self.data_ingestion_config.feature_store_file_path)
        ok.makedirs(feature_store_dir,exist_ok=True)

        df.to_csv(path_or_buf=self.data_ingestion_config.feature_store_file_path,index=False,header=True)

        train_df,test_df=train_test_split(df,test_size=self.data_ingestion_config.test_size)

        dataset_dir=os.path.dirname(self.data_ingestion_config.train_file_path)
        os.makedirs(dataset_dir,exist_ok=True)

        train_df.to_csv(path_or_buf=self.data_ingestion_config.train_file_path,index=False,header=True)
        test_df.to_csv(path_or_buf=self.data_ingestion_config.test_file_path,index=False,header=True)

        data_ingestion_artifact = artifact_entity.DataIngestionArtifact(
            feature_store_path=self.data_ingestion_config.feature_store_file_path, 
            train_file_path=self.data_ingestion_config.train_file_path,
            test_file_path=self.data_ingestion_config.test_file_path)

        return data_ingestion_artifact






