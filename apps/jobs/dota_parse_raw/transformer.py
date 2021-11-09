from pyspark.sql.functions import col, explode
from shared.udfs import get_array_type_ohe_udf


class DotaRawTransformer:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.entities = []
    
    def _transform_heroes(self, heroes_df):
        exploded_skills = heroes_df.select(explode("skills").alias("exploded_skills"))        
        unique_skills = {row[0]: idx for idx, row in enumerate(exploded_skills.distinct().collect())}
        ohe_heroes_df = heroes_df.select([
                        "id", 
                        "name",
                        "localized_name",
                        get_array_type_ohe_udf(mapping=unique_skills, col="skills").alias("skills_ohe")
                        ])
        columns = ohe_heroes_df.columns[:-1]
        splitted_columns = [(col("skills_ohe")[idx]).alias(name) for name, idx in unique_skills.items()]
        return ohe_heroes_df.select(columns + splitted_columns)
    
    def _transform_lobbies(self, lobbies_df):
        return lobbies_df

    def _transform_regions(self, regions_df):
        return regions_df
    
    def transform(self, entities):
        dataframes = [
                        self._transform_heroes(entities["heroes"]), 
                        self._transform_lobbies(entities["lobbies"]),
                        self._transform_regions(entities["regions"])
                    ]
        
        names = ["heroes", "lobbies", "regions"]
        entities.update({name: dataframe for name, dataframe in zip(names, dataframes)})
