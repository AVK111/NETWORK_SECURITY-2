from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.exception.exception import NetworkSytemException
from networksecurity.logging.logger import logging
import sys
from networksecurity.components.data_validation import DataValidation
from networksecurity.entity.config_entity import DataIngestionConfig, DataValidConfig
from networksecurity.entity.config_entity import TrainingPipleineConfig
if __name__=='__main__':
    try:
        trainingpipelineconfig=TrainingPipleineConfig()
        dataingestionconfig=DataIngestionConfig(training_pipeline_config=trainingpipelineconfig)
        data_ingestion=DataIngestion(data_ingestion_config=dataingestionconfig)
        logging.info("Starting the data ingestion")
        dataingestionartifact=data_ingestion.initiate_data_ingestion()
        logging.info("Data ingestion completed and artifact is created")
        print(dataingestionconfig)
        data_validation_config=DataValidConfig(trainingpipelineconfig)
        # create DataValidation instance with the ingestion artifact and validation config
        data_validation = DataValidation(dataingestionartifact, data_validation_config)
        logging.info("Initialize the data validation")
        data_validation_Artifact = data_validation.initiate_data_validation()
        logging.info("data Validation completed")
        print(data_validation_Artifact)

    except Exception as e:
        raise NetworkSytemException(e,sys) from e