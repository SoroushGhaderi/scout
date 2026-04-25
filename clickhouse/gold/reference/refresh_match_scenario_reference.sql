INSERT INTO {{database}}.match_scenario_reference (
    match_id,
    match_date,
    match_time_utc,
    match_time_utc_date,
    match_round,
    coverage_level,
    league_id,
    league_name,
    league_round_name,
    parent_league_id,
    parent_league_name,
    parent_league_season,
    parent_league_tournament_id,
    country_code,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    match_started,
    match_finished,
    full_score,
    home_score,
    away_score,
    all_scenario_ids,
    available_scenario_ids,
    unavailable_scenario_ids,
    scenario_count,
    available_scenario_count,
    has_any_scenario
)
SELECT
    br.match_id,
    br.match_date,
    br.match_time_utc,
    br.match_time_utc_date,
    br.match_round,
    br.coverage_level,
    br.league_id,
    br.league_name,
    br.league_round_name,
    br.parent_league_id,
    br.parent_league_name,
    br.parent_league_season,
    br.parent_league_tournament_id,
    br.country_code,
    br.home_team_id,
    br.home_team_name,
    br.away_team_id,
    br.away_team_name,
    br.match_started,
    br.match_finished,
    br.full_score,
    br.home_score,
    br.away_score,
    %(all_item_ids)s AS all_scenario_ids,
    arraySort(ifNull(available.available_scenario_ids, CAST([], 'Array(String)'))) AS available_scenario_ids,
    arraySort(
        arrayFilter(
            scenario_id -> NOT has(
                ifNull(available.available_scenario_ids, CAST([], 'Array(String)')),
                scenario_id
            ),
            %(all_item_ids)s
        )
    ) AS unavailable_scenario_ids,
    toUInt16(length(%(all_item_ids)s)) AS scenario_count,
    toUInt16(length(ifNull(available.available_scenario_ids, CAST([], 'Array(String)')))) AS available_scenario_count,
    toUInt8(length(ifNull(available.available_scenario_ids, CAST([], 'Array(String)'))) > 0) AS has_any_scenario
FROM bronze.match_reference FINAL AS br
{{available_items_join}}
WHERE br.match_id > 0
