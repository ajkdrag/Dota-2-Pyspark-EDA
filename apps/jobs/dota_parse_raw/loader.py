from os import path


class DotaRawLoader:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.entities = []

    def _load_entity(self, name, dataframe):
        """write dataframe as parquet into the curated layer"""
        out_file = path.join(self.config.get("curated"), name)
        (dataframe.write.option("header", "true").parquet(out_file))

    def load(self, entities):
        """wrapper func for writing dataframes post curation"""
        for name, dataframe in entities.items():
            self._load_entity(name, dataframe)
