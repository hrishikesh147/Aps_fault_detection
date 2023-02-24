from sensor.entity import config_entity
from sensor.entity import artifact_entity
from sensor.exception import SensorException
from typing import Optional
from scipy.stats import ks_2samp

class DataValidation:

    def __init__(self,
                    data_validation_config:config_entity.DataValidationConfig,
                    data_ingestion_artifact:artifact_entity.DataIngestionArtifact):
        try:
            logging.info(f"{'>'*20} Data Validation {'<'*20} ")
            self.data_validation_config=data_validation_config
            self.data_ingestion_artifact-data_ingestion_artifact
            self.validation_error=dict()
        except Exception as e:
            raise SensorException(e, sys)

    def drop_missing_values_columns(self,df:pd.DataFrame,report_key_name:str)->Optional[pd.DataFrame]:
        try:
            threshold=self.data_validation_config.missing_threshold
            null_report=df.isna().sum()/df.shape[0]
            drop_column_names=null_report[null_report>threshold].index

            self.validation_error[report_key_name]=list(drop_column_names)
            df.drop(list(drop_column_names),axis=1,inplace=True)

            if len(df.columns)==0:
                return None
            return df
        except Exception as e:
            raise SensorException(e, sys)

    def is_required_column_exists(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str)->bool:
        try:
            base_columns=base_df.columns
            current_columns=current_df.columns

            missing_columns=[]
            for column in base_columns:
                if column not in current_columns:
                    missing_columns.append(column)

            if len(missing_columns)>0:
                self.validation_error[report_key_name]=missing_columns
                return False
            return True

        except Exception as e:
            raise SensorException(e, sys)

    def data_drift(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_file_path:str):
        try:
            drift_report=dict()
            base_columns=base_df.column
            current_columns=current_df.column

            for i in base_columns:
                base_data,current_data=base_df[i],current_df[i]

                same_distribution=ks_2samp(base_data,current_data)

                if same_distribution.pvalue>0.05:
                    drift_report[i]={
                        "pvalues":float(same_distribution.pvalue),
                        "same_distribution": True
                    }

                else:
                    drift_report[base_column]={
                        "pvalues":float(same_distribution.pvalue),
                        "same_distribution":False
                    }

                self.validation_error[report_key_name]=drift_report
            except Exception as e:
                raise SensorException(e,sys)




        



