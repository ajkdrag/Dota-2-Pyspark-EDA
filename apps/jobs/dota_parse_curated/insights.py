import pyspark.sql.functions as F


def top_k_most_picked_heroes(match_hero_names_df, k=10):
    return match_hero_names_df.groupBy("hero").count().orderBy(F.desc("count")).limit(k)


def top_k_most_picked_heroes_radiant(match_hero_names_df, k=10):
    return top_k_most_picked_heroes(
        match_hero_names_df.filter(F.col("team") == "radiant"), k=k
    )


def top_k_most_picked_heroes_dire(match_hero_names_df, k=10):
    return top_k_most_picked_heroes(
        match_hero_names_df.filter(F.col("team") == "dire"), k=k
    )


def top_k_heroes_with_highest_win_rates(match_hero_names_df, match_details_df, k=10):
    merged_df = match_hero_names_df.join(
        match_details_df, on=[match_hero_names_df.match_id == match_details_df.match_id]
    ).select(
        [
            match_hero_names_df.match_id,
            match_hero_names_df.hero,
            match_hero_names_df.team,
            "winner",
        ]
    )

    return (
        merged_df.withColumn(
            "score",
            F.when(F.col("winner") == F.col("team"), F.lit(1)).otherwise(F.lit(0)),
        )
        .groupBy("hero")
        .agg(
            F.count("match_id").alias("total_picks"),
            F.sum("score").alias("total_wins"),
        )
        .withColumn("win_rate", (100 * F.col("total_wins")) / F.col("total_picks"))
        .orderBy(F.desc("win_rate"))
        .limit(k)
    )


def top_k_heroes_in_most_wins(match_hero_names_df, match_details_df, k=10):
    return (
        match_hero_names_df.join(
            match_details_df,
            on=[
                match_hero_names_df.match_id == match_details_df.match_id,
                match_hero_names_df.team == match_details_df.winner,
            ],
        )
        .groupBy("hero")
        .count()
        .orderBy(F.desc("count"))
        .limit(k)
    )


def get_all_insights(entities):
    match_hero_names_df = entities["match_hero_names"]
    match_details_df = entities["match_details"]

    top_k_most_picked_heroes_df = top_k_most_picked_heroes(match_hero_names_df)
    print("top_k_most_picked_heroes")
    top_k_most_picked_heroes_df.show()

    top_k_most_picked_heroes_radiant_df = top_k_most_picked_heroes_radiant(
        match_hero_names_df
    )
    print("top_k_most_picked_heroes_radiant")
    top_k_most_picked_heroes_radiant_df.show()

    top_k_most_picked_heroes_dire_df = top_k_most_picked_heroes_dire(
        match_hero_names_df
    )
    print("top_k_most_picked_heroes_dire")
    top_k_most_picked_heroes_dire_df.show()

    top_k_heroes_in_most_wins_df = top_k_heroes_in_most_wins(
        match_hero_names_df, match_details_df
    )
    print("top_k_heroes_in_most_wins")
    top_k_heroes_in_most_wins_df.show()

    top_k_heroes_with_highest_win_rates_df = top_k_heroes_with_highest_win_rates(
        match_hero_names_df, match_details_df
    )
    print("top_k_heroes_with_highest_win_rates")
    top_k_heroes_with_highest_win_rates_df.show()
