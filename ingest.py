from config_manager import ConfigManager
from schema_manager import SchemaManager
from table_processor import TableProcessor
from etl_utils import add_hash_key
import logging

__all__ = ["add_hash_key", "ETLPipeline"]


class ETLPipeline:
    def __init__(self, config: ConfigManager, schema: SchemaManager):
        self.config = config
        self.schema = schema

    def run(self) -> None:
        write_mode = "append"
        historical_load = True
        for table in self.config.TABLES:
            if self.config.TABLE_PROCESSING_CONFIG.get(table, False):
                TableProcessor(table, write_mode, historical_load, self.config, self.schema).process()
            else:
                logging.info(f"Skipping processing for {table}")
        logging.info("ETL process completed successfully.")


if __name__ == "__main__":
    config = ConfigManager()
    schema = SchemaManager()
    ETLPipeline(config, schema).run()
