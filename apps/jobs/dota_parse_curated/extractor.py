from os import path
from jobs.dota_parse_curated import BaseExtractor


class DotaCuratedExtractor(BaseExtractor):
    def __init__(self, spark, config):
        super().__init__(spark, config)

    def _extract_ohe_heroes(self):
        """extract one-hot-encoded heroes dataframe from parquet"""
        heroes_file = path.join(self.config.get("curated"), "ohe_heroes/")
        return self._extract_parquet(heroes_file)

    def _extract_match_hero_names(self):
        """extract match_hero dataframe from parquet"""
        match_hero_names_file = path.join(
            self.config.get("curated"), "match_hero_names/"
        )
        return self._extract_parquet(match_hero_names_file)

    def _extract_match_details(self):
        """extract match_details dataframe from parquet"""
        match_details_file = path.join(self.config.get("curated"), "match_details/")
        return self._extract_parquet(match_details_file)

    def _extract_regions(self):
        """extract regions dataframe from json"""
        regions_file = path.join(self.config.get("curated"), "regions/")
        return self._extract_parquet(regions_file)

    def extract(self, entities):
        """wrapper func to extract dataframes from source files"""
        ohe_heroes_df = self._extract_ohe_heroes()
        match_hero_names_df = self._extract_match_hero_names()
        match_details_df = self._extract_match_details()
        regions_df = self._extract_regions()

        dataframes = [
            ohe_heroes_df,
            match_details_df,
            match_hero_names_df,
            regions_df,
        ]
        names = ["ohe_heroes", "match_details", "match_hero_names", "regions"]
        entities.update({name: dataframe for name, dataframe in zip(names, dataframes)})
