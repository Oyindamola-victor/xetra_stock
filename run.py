"""
Entrypoint for running the Xetra ETL application
"""
import logging 
import logging.config

import yaml

def main():
    """
    Entrypoint to run the xetra ETL Job
    """
    
    # Parsing YAML File
    config_path = "/home/damola/Documents/ETL_Pipeline/xetra_stock/configs/xetra_report_config.yml"
    config = yaml.safe_load(open(config_path))
    # configure logging
    
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")
    
    
if __name__ == "__main__":
    main()