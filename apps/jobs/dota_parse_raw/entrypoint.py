from jobs.dota_parse_raw.extractor import DotaRawExtractor
from jobs.dota_parse_raw.transformer import DotaRawTransformer
from jobs.dota_parse_raw.loader import DotaRawLoader


def run(spark, config):
    print("Running the ETL job")
    
    raw_extractor = DotaRawExtractor(spark, config) 
    raw_transformer = DotaRawTransformer(spark, config)
    raw_loader = DotaRawLoader(spark, config)
        
    entities = {}
    raw_extractor.extract(entities)
    raw_transformer.transform(entities)
    raw_loader.load(entities)
    
    print("Finished the ETL job")
