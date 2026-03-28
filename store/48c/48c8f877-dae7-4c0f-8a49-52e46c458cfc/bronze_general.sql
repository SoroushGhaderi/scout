ATTACH TABLE _ UUID 'f44b87a2-1c2c-402b-9089-3f81e4964482'
(
    `match_id` Int32,
    `match_round` Nullable(String),
    `team_color_dark_mode_home` Nullable(String),
    `team_color_dark_mode_away` Nullable(String),
    `team_color_light_mode_home` Nullable(String),
    `team_color_light_mode_away` Nullable(String),
    `league_id` Nullable(Int32),
    `league_name` Nullable(String),
    `league_round_name` Nullable(String),
    `parent_league_id` Nullable(Int32),
    `country_code` Nullable(String),
    `parent_league_name` Nullable(String),
    `parent_league_season` Nullable(String),
    `parent_league_tournament_id` Nullable(Int32),
    `home_team_name` Nullable(String),
    `home_team_id` Nullable(Int32),
    `away_team_name` Nullable(String),
    `away_team_id` Nullable(Int32),
    `coverage_level` Nullable(String),
    `match_time_utc` Nullable(String),
    `match_time_utc_date` Nullable(String),
    `match_started` UInt8,
    `match_finished` UInt8,
    `full_score` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY match_id
SETTINGS index_granularity = 8192
