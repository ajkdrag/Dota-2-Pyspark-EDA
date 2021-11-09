from os import path
from jobs.dota_parse_raw import BaseExtractor


class DotaRawExtractor(BaseExtractor):
    def __init__(self, spark, config):
        super().__init__(spark, config)
     
    def _extract_heroes(self):
        heroes_file = path.join(self.config.get("raw"), "heroes.json")
        return self._extract_json(heroes_file, multiline=True)

    def _extract_matches(self, extra_columns=None):
        columns = ["radiant_dire", 
                    "cluster_id", 
                    "game_mode", 
                    "game_type"]
        columns += (extra_columns if extra_columns else [])
        matches_file = path.join(self.config.get("raw"), "dota2Test.csv")
        return self._extract_csv(matches_file, columns, header=False)
    
    def _extract_lobbies(self):
        lobbies_file = path.join(self.config.get("raw"), "lobbies.json")
        return self._extract_json(lobbies_file, multiline=True)

    def _extract_regions(self):
        regions_file = path.join(self.config.get("raw"), "regions.json")
        return self._extract_json(regions_file, multiline=True)

    def extract(self, entities):
        exploded_heroes_df = self._basic_explode(self._extract_heroes(), "heroes", only_exploded=True)
        exploded_lobbies_df = self._basic_explode(self._extract_lobbies(), "lobbies", only_exploded=True)
        exploded_regions_df = self._basic_explode(self._extract_regions(), "regions", only_exploded=True)
        
        id_name_df = (
                exploded_heroes_df
                .sort("id")
                .select("name")
                )
        hero_names = [row[0] for row in id_name_df.collect()]

        matches_df = self._extract_matches(hero_names)
        
        dataframes = [exploded_heroes_df, exploded_lobbies_df, exploded_regions_df, matches_df]
        names = ["heroes", "lobbies", "regions", "matches"]
        entities.update({name: dataframe for name, dataframe in zip(names, dataframes)})
