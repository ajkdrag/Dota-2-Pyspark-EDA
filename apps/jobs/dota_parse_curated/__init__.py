class BaseExtractor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def _extract_parquet(self, parquet_file):
        """read parquet file"""
        return self.spark.read.parquet(parquet_file)
