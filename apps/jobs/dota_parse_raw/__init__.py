from pyspark.sql.functions import col, explode 


class BaseExtractor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    def _basic_explode(self, json_df, column, only_exploded=False):
        exploded_df = json_df.withColumn("exploded", explode(col(column)))    
        if only_exploded:
            exploded_df = exploded_df.select("exploded.*")
        return exploded_df
    
    def _extract_json(self, json_file, multiline=False):
        return (
                self.spark.read
                .option("multiline", "true" if multiline else "false")
                .json(json_file)
                )

    def _extract_csv(self, csv_file, columns, header=False):
        return (
                self.spark.read
                .format("csv")
                .option("header", "true" if header else "false")
                .load(csv_file)
                .toDF(*columns)
                )
