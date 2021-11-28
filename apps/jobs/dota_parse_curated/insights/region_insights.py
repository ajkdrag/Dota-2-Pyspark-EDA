import pyspark.sql.functions as F
from pyspark.sql.window import Window


def top_k_regions_with_most_plays(match_details_df, regions_df, k=5):
    cluster_wise_matches_df = match_details_df.groupBy("cluster_id").count()
    return (
        cluster_wise_matches_df.join(regions_df, on=["cluster_id"], how="inner")
        .groupBy("region")
        .agg(F.sum("count").alias("matches"))
        .orderBy(F.desc("matches"))
        .limit(k)
    )


def top_k_picked_heroes_region_wise(
    match_details_df, regions_df, match_hero_names_df, k=1
):
    match_id_region_map_df = match_details_df.join(
        regions_df, on=["cluster_id"], how="inner"
    ).select("match_id", "region")

    hero_region_map_df = match_hero_names_df.join(
        match_id_region_map_df, on=["match_id"], how="inner"
    ).select("hero", "region")

    group_counts = hero_region_map_df.groupBy("region", "hero").count()
    w = Window().partitionBy("region").orderBy(F.desc("count"))
    return (
        group_counts.withColumn("rank", F.dense_rank().over(w))
        .filter(F.col("rank") <= k)
        .orderBy(F.desc("count"))
    )


def get_all_region_insights(entities):
    match_details_df = entities["match_details"]
    match_hero_names_df = entities["match_hero_names"]
    regions_df = entities["regions"]

    top_k_regions_with_most_plays_df = top_k_regions_with_most_plays(
        match_details_df, regions_df
    )
    top_k_picked_heroes_region_wise_df = top_k_picked_heroes_region_wise(
        match_details_df, regions_df, match_hero_names_df
    )
    entities["insight_regions_with_most_plays"] = top_k_regions_with_most_plays_df
    entities[
        "insight_most_picked_heroes_region_wise"
    ] = top_k_picked_heroes_region_wise_df
