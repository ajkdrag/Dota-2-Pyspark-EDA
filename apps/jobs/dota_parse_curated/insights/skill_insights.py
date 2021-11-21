import pyspark.sql.functions as F


def top_k_most_picked_skills(match_hero_names_df, ohe_heroes_df, k=5):
    skills_df = match_hero_names_df.join(
        ohe_heroes_df, on=[match_hero_names_df.hero == ohe_heroes_df.name]
    ).select(ohe_heroes_df.columns[3:])
    skills = skills_df.columns
    skills_df_agg = skills_df.select([F.sum(col).alias(col) for col in skills])
    list_skill_picks = [
        F.struct(F.lit(col).alias("skill"), F.col(col).alias("num_picks"))
        for col in skills
    ]
    return (
        skills_df_agg.select(F.explode(F.array(list_skill_picks)).alias("exploded"))
        .select("exploded.*")
        .orderBy(F.desc("num_picks"))
        .limit(k)
    )


def top_k_skills_in_most_wins(
    match_hero_names_df, match_details_df, ohe_heroes_df, k=5
):
    return top_k_most_picked_skills(
        match_hero_names_df.join(
            match_details_df,
            on=[
                match_hero_names_df.match_id == match_details_df.match_id,
                match_hero_names_df.team == match_details_df.winner,
            ],
        ),
        ohe_heroes_df,
        k=k,
    )


def top_k_skills_with_highest_win_rates(
    match_hero_names_df, match_details_df, ohe_heroes_df, k=10
):
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

    hero_scores_df = merged_df.withColumn(
        "score",
        F.when(F.col("winner") == F.col("team"), F.lit(1)).otherwise(F.lit(0)),
    )

    skills = ohe_heroes_df.columns[3:]

    skill_scores_df = hero_scores_df.join(
        ohe_heroes_df, on=[hero_scores_df.hero == ohe_heroes_df.name]
    )

    list_skill_picks = [
        F.struct(
            F.lit(col).alias("skill"),
            (F.col("score") * F.col(col)).alias("wins"),
            (F.col(col)).alias("picks"),
        )
        for col in skills
    ]
    return (
        skill_scores_df.select(F.explode(F.array(list_skill_picks)).alias("exploded"))
        .select("exploded.*")
        .groupBy("skill")
        .agg(
            F.sum("picks").alias("total_picks"),
            F.sum("wins").alias("total_wins"),
        )
        .withColumn("win_rate", (100 * F.col("total_wins")) / F.col("total_picks"))
        .orderBy(F.desc("win_rate"))
        .limit(k)
    )


def get_all_skill_insights(entities):
    match_hero_names_df = entities["match_hero_names"]
    ohe_heroes_df = entities["ohe_heroes"]
    match_details_df = entities["match_details"]

    top_k_most_picked_skills_df = top_k_most_picked_skills(
        match_hero_names_df, ohe_heroes_df
    )

    top_k_skills_in_most_wins_df = top_k_skills_in_most_wins(
        match_hero_names_df, match_details_df, ohe_heroes_df
    )

    top_k_skills_with_highest_win_rates_df = top_k_skills_with_highest_win_rates(
        match_hero_names_df, match_details_df, ohe_heroes_df
    )

    entities["insight_most_picked_skills"] = top_k_most_picked_skills_df
    entities["insight_skills_in_most_wins"] = top_k_skills_in_most_wins_df
    entities["insight_skills_with_highest_wr"] = top_k_skills_with_highest_win_rates_df
