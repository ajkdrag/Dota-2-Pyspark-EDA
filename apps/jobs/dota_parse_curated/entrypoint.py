from jobs.dota_parse_curated.extractor import DotaCuratedExtractor
from jobs.dota_parse_curated.insights import get_all_insights 


def run(spark, config):
    """entrypoint for the submitted spark job"""

    print("----- Running the Dota2 curation job -----")
    raw_extractor = DotaCuratedExtractor(spark, config)

    entities = {}
    raw_extractor.extract(entities)
    get_all_insights(entities)
    print("----- Finished the Dota2 curation job -----")
