-- Queries for the lakehouse SQL analytics endpoint (T-SQL, read-only)
-- Same Delta tables the notebook wrote, now queried with plain SQL. No copy, no load step.

-- 1. Most efficient QBs of 2024 (EPA per dropback)
SELECT TOP 10 player_name, team_name, passing_yards, passing_tds, interceptions, epa_per_dropback
FROM gold_qb_season
WHERE season = 2024
ORDER BY epa_per_dropback DESC;

-- 2. Fantasy WR1-WR12 with points per game
SELECT label, player_name, games, ppr_points, ppr_per_game
FROM gold_fantasy_leaders
WHERE season = 2024 AND position = 'WR' AND position_rank <= 12
ORDER BY position_rank;

-- 3. Division standings (window function in T-SQL)
SELECT team_conf, team_division, team_name, wins, losses, ties, point_diff,
       RANK() OVER (PARTITION BY team_division ORDER BY wins DESC, point_diff DESC) AS division_rank
FROM gold_team_records
WHERE season = 2024
ORDER BY team_division, division_rank;

-- 4. Dedup / "latest record per key" pattern: each player's best fantasy week of 2024
WITH ranked AS (
    SELECT player_name, position, team, week, fantasy_points_ppr,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY fantasy_points_ppr DESC) AS rn
    FROM silver_player_week
    WHERE season = 2024 AND season_type = 'REG'
)
SELECT TOP 15 player_name, position, team, week, fantasy_points_ppr
FROM ranked
WHERE rn = 1
ORDER BY fantasy_points_ppr DESC;
