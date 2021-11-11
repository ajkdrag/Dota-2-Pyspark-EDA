from pyspark.sql.functions import (
    lit,
    col,
    coalesce,
    explode,
    create_map,
    array,
    monotonically_increasing_id,
)
from shared.udfs import get_array_type_ohe_udf, get_array_type_inv_ohe_udf
from itertools import chain


class DotaRawTransformer:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.entities = []

    def _transform_heroes(self, heroes_df):
        """transform skills for hero as one-hot-encoded columns"""
        exploded_skills = heroes_df.select(explode("skills").alias("exploded_skills"))
        unique_skills = {
            row[0]: idx for idx, row in enumerate(exploded_skills.distinct().collect())
        }
        ohe_heroes_df = heroes_df.select(
            [
                "id",
                "name",
                "localized_name",
                get_array_type_ohe_udf(mapping=unique_skills, col="skills").alias(
                    "skills_ohe"
                ),
            ]
        )
        columns = ohe_heroes_df.columns[:-1]
        splitted_columns = [
            (col("skills_ohe")[idx]).alias(name) for name, idx in unique_skills.items()
        ]
        ohe_heroes_df = ohe_heroes_df.select(columns + splitted_columns)
        return [("ohe_heroes", ohe_heroes_df)]

    def _transform_matches(self, matches_df):
        """transform matches by adding pk column, mapping column ids to values and merge one-hot-encoded columns"""
        pk_added_matches_df = matches_df.withColumn(
            "match_id", monotonically_increasing_id()
        )
        mapping = {"-1": "dire", "1": "radiant"}
        mapping_rdd = create_map([lit(item) for item in chain(*mapping.items())])
        mapped_win_col_df = pk_added_matches_df.withColumn(
            "winner", coalesce(mapping_rdd[col("winner")], lit("draw"))
        )
        match_details_df = mapped_win_col_df.select(
            "match_id", "cluster_id", "game_mode", "game_type", "winner"
        )

        all_columns = set(matches_df.columns)
        columns_to_ignore = set(match_details_df.columns)
        hero_id_ohe_cols = [col(column) for column in (all_columns - columns_to_ignore)]
        merged_hero_ids_df = pk_added_matches_df.withColumn(
            "heroes", array(hero_id_ohe_cols)
        ).select("match_id", "heroes")
        return [
            ("match_details", match_details_df),
            ("merged_hero_ids", merged_hero_ids_df),
        ]

    def _transform_regions(self, regions_df):
        """rename columns for cluster regions"""
        return [("regions", regions_df.toDF("cluster_id", "region"))]

    def _transform_lobbies(self, lobbies_df):
        """transorm lobbies"""
        return [("lobbies", lobbies_df)]

    def _create_match_hero_xlkp_df(self, merged_hero_ids_df, ohe_heroes_df):
        """map hero ids to hero names and denormalize the dataframe"""
        id_name_df = ohe_heroes_df.sort("id").select("name")
        hero_names_mapping = {
            idx: row[0] for idx, row in enumerate(id_name_df.collect())
        }
        hero_team_mapping = {"-1": "dire", "1": "radiant"}
        exploded_match_hero_names_df = merged_hero_ids_df.withColumn(
            "heroes",
            explode(
                get_array_type_inv_ohe_udf(
                    hero_names_mapping, hero_team_mapping, "heroes"
                )
            ),
        )
        filtered_match_hero_names_df = exploded_match_hero_names_df.select(
            "match_id",
            (col("heroes")[0]).alias("hero"),
            (col("heroes")[1]).alias("team"),
        ).filter(col("team") != "None")
        return [("match_hero_names", filtered_match_hero_names_df)]

    def _lkp_match_lobby(self, match_details_df, lobbies_df):
        """join lobby and matches to get lobby name for each match"""
        lkp_df = lobbies_df.withColumn("id", col("id").cast("int") + 1)
        match_details_df = match_details_df.withColumn(
            "game_mode", col("game_mode").cast("int")
        )
        match_details_df = (
            match_details_df.join(
                lkp_df, on=(match_details_df.game_mode == lkp_df.id), how="inner"
            )
            .select(match_details_df.columns + [col("name").alias("lobby")])
            .drop("game_type", "game_mode")
        )
        return [("match_details", match_details_df)]

    def simple_transform(self, entities):
        """func for transormations that are self-sustained i.e no joins/lookups"""
        names_and_dataframes = [
            *self._transform_heroes(entities["heroes"]),
            *self._transform_lobbies(entities["lobbies"]),
            *self._transform_regions(entities["regions"]),
            *self._transform_matches(entities["matches"]),
        ]
        entities.update({name: dataframe for name, dataframe in names_and_dataframes})

    def transform(self, entities):
        """wrapper func for transforming dataframes"""
        self.simple_transform(entities)
        match_details_df = entities["match_details"]
        merged_hero_ids_df = entities["merged_hero_ids"]
        ohe_heroes_df = entities["ohe_heroes"]
        lobbies_df = entities["lobbies"]
        names_and_dataframes = [
            *self._create_match_hero_xlkp_df(merged_hero_ids_df, ohe_heroes_df),
            *self._lkp_match_lobby(match_details_df, lobbies_df),
        ]

        del entities["merged_hero_ids"]
        del entities["matches"]
        del entities["heroes"]
        del entities["lobbies"]
        entities.update({name: dataframe for name, dataframe in names_and_dataframes})
