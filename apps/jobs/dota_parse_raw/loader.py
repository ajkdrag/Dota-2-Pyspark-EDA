from os import path


class DotaRawLoader:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.entities = []

    def _load_entity(self, name, dataframe):
        out_file = path.join(self.config.get("curated"), name)
        (
            dataframe
            .write
            .option("header", "true")
            .parquet(out_file)
        )
    
    def load(self, entities):
        for name, dataframe in entities.items(): 
            self._load_entity(name, dataframe)
