from jobs.dota_parse_curated.insights import (
    hero_insights,
    skill_insights,
    region_insights,
)


def get_all_insights(entities):
    hero_insights.get_all_hero_insights(entities)
    skill_insights.get_all_skill_insights(entities)
    region_insights.get_all_region_insights(entities)
