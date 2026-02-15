from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.exception.exception import NetworkSytemException
from networksecurity.logging.logger import logging
import sys
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.config_entity import TrainingPipleineConfig
if __name__=='__main__':
    try:
        trainingpipelineconfig=TrainingPipleineConfig()
        dataingestionconfig=DataIngestionConfig(training_pipeline_config=trainingpipelineconfig)
        data_ingestion=DataIngestion(data_ingestion_config=dataingestionconfig)
        logging.info("Starting the data ingestion")
        dataingestionartifact=data_ingestion.initiate_data_ingestion()
        print(dataingestionartifact)

    except Exception as e:
        raise NetworkSytemException(e,sys) from e