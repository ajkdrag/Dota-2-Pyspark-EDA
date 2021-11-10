from jobs.dota_parse_raw.extractor import DotaRawExtractor
from jobs.dota_parse_raw.transformer import DotaRawTransformer
from jobs.dota_parse_raw.loader import DotaRawLoader


def run(spark, config):
    """entrypoint for the submitted spark job"""
    
    print("----- Running the Dota2 ETL job -----")
    raw_extractor = DotaRawExtractor(spark, config) 
    raw_transformer = DotaRawTransformer(spark, config)
    raw_loader = DotaRawLoader(spark, config)
        
    entities = {}
    raw_extractor.extract(entities)
    raw_transformer.transform(entities)
    raw_loader.load(entities)
    print("----- Finished the Dota2 ETL job -----")
