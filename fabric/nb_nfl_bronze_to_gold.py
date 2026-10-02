# Fabric notebook: bronze (raw CSV files) -> silver (clean Delta) -> gold (analytics Delta)
# Each CELL block is one notebook cell. The lakehouse lh_nfl is attached as the default lakehouse.

# ======================= CELL 1: config + helper =======================
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE = "Files/nfl"   # the OneLake shortcut pointing at ADLS bronze/nfl
                       # (manual-upload fallback: create Files/nfl/<table>/ folders and upload the CSVs there)

def latest_folder(table: str) -> str:
    """ADF lands each run under <table>/ingest_date=YYYY-MM-DD/. Only process the newest run."""
    base = f"{BRONZE}/{table}"
    runs = sorted(f.name.rstrip("/") for f in notebookutils.fs.ls(base) if f.name.startswith("ingest_date="))
    if not runs:            # manual-upload fallback: files sit directly in the table folder
        return base
    return f"{base}/{runs[-1]}"

def read_bronze(table: str):
    path = latest_folder(table)
    print(f"Reading {path}")
    return spark.read.option("header", True).option("inferSchema", True).csv(f"{path}/*.csv")


# ======================= CELL 2: silver_player_week (incremental MERGE) =======================
# One row per player per game-week. Selected columns, clean types, lineage column.
player_week = (
    read_bronze("player_stats")
    .select(
        "player_id",
        F.col("player_display_name").alias("player_name"),
        "position",
        F.col("recent_team").alias("team"),
        F.col("season").cast("int"),
        F.col("week").cast("int"),
        "season_type",
        "opponent_team",
        F.col("completions").cast("int"),
        F.col("attempts").cast("int"),
        F.col("passing_yards").cast("int"),
        F.col("passing_tds").cast("int"),
        F.col("interceptions").cast("int"),
        F.col("sacks").cast("int"),
        F.col("passing_epa").cast("double"),
        F.col("carries").cast("int"),
        F.col("rushing_yards").cast("int"),
        F.col("rushing_tds").cast("int"),
        F.col("receptions").cast("int"),
        F.col("targets").cast("int"),
        F.col("receiving_yards").cast("int"),
        F.col("receiving_tds").cast("int"),
        F.col("fantasy_points").cast("double"),
        F.col("fantasy_points_ppr").cast("double"),
    )
    .dropDuplicates(["player_id", "season", "week", "season_type"])
    .withColumn("_ingested_at", F.current_timestamp())
)

# Upsert instead of overwrite: rerunning is safe (idempotent) and loading a new season just adds rows.
if spark.catalog.tableExists("silver_player_week"):
    (DeltaTable.forName(spark, "silver_player_week").alias("t")
        .merge(player_week.alias("s"),
               "t.player_id = s.player_id AND t.season = s.season "
               "AND t.week = s.week AND t.season_type = s.season_type")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    player_week.write.format("delta").saveAsTable("silver_player_week")

print(spark.table("silver_player_week").groupBy("season").count().orderBy("season").collect())


# ======================= CELL 3: silver_teams + silver_games (small, full overwrite) =======================
teams = (
    read_bronze("teams")
    .select("team_abbr", "team_name", "team_nick", "team_conf", "team_division", "team_color")
)
teams.write.format("delta").mode("overwrite").saveAsTable("silver_teams")

games = (
    read_bronze("games")
    .select(
        "game_id",
        F.col("season").cast("int"),
        "game_type",
        F.col("week").cast("int"),
        F.to_date("gameday").alias("gameday"),
        "away_team",
        F.col("away_score").cast("int"),
        "home_team",
        F.col("home_score").cast("int"),
        F.col("spread_line").cast("double"),
        "roof",
        "surface",
        "stadium",
    )
    .where(F.col("home_score").isNotNull())   # drop games not played yet
)
games.write.format("delta").mode("overwrite").saveAsTable("silver_games")


# ======================= CELL 4: gold_qb_season =======================
qb = (
    spark.table("silver_player_week")
    .where((F.col("position") == "QB") & (F.col("season_type") == "REG"))
    .groupBy("season", "player_id", "player_name")
    .agg(
        F.countDistinct("week").alias("games"),
        F.max_by("team", "week").alias("team"),   # team they finished the season on
        F.sum("completions").alias("completions"),
        F.sum("attempts").alias("attempts"),
        F.sum("passing_yards").alias("passing_yards"),
        F.sum("passing_tds").alias("passing_tds"),
        F.sum("interceptions").alias("interceptions"),
        F.sum("sacks").alias("sacks"),
        F.sum("passing_epa").alias("total_epa"),
        F.sum("fantasy_points").alias("fantasy_points"),
    )
    .where(F.col("attempts") >= 200)          # qualified starters only
    .withColumn("completion_pct", F.round(F.col("completions") / F.col("attempts") * 100, 1))
    .withColumn("epa_per_dropback", F.round(F.col("total_epa") / (F.col("attempts") + F.col("sacks")), 3))
    .join(spark.table("silver_teams").select(F.col("team_abbr").alias("team"), "team_name", "team_conf"),
          "team", "left")
)
qb.write.format("delta").mode("overwrite").saveAsTable("gold_qb_season")


# ======================= CELL 5: gold_fantasy_leaders (window function) =======================
season_totals = (
    spark.table("silver_player_week")
    .where((F.col("season_type") == "REG") & F.col("position").isin("QB", "RB", "WR", "TE"))
    .groupBy("season", "player_id", "player_name", "position")
    .agg(
        F.countDistinct("week").alias("games"),
        F.round(F.sum("fantasy_points_ppr"), 1).alias("ppr_points"),
    )
    .withColumn("ppr_per_game", F.round(F.col("ppr_points") / F.col("games"), 1))
)
rank_window = Window.partitionBy("season", "position").orderBy(F.desc("ppr_points"))
fantasy = (
    season_totals
    .withColumn("position_rank", F.row_number().over(rank_window))
    .withColumn("label", F.concat(F.col("position"), F.col("position_rank")))   # e.g. WR1, RB12
    .where(F.col("position_rank") <= 36)
)
fantasy.write.format("delta").mode("overwrite").saveAsTable("gold_fantasy_leaders")


# ======================= CELL 6: gold_team_records (unpivot home/away) =======================
g = spark.table("silver_games").where(F.col("game_type") == "REG")
home = g.select("season", F.col("home_team").alias("team"),
                F.col("home_score").alias("pf"), F.col("away_score").alias("pa"))
away = g.select("season", F.col("away_team").alias("team"),
                F.col("away_score").alias("pf"), F.col("home_score").alias("pa"))
records = (
    home.unionByName(away)
    .groupBy("season", "team")
    .agg(
        F.sum(F.when(F.col("pf") > F.col("pa"), 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("pf") < F.col("pa"), 1).otherwise(0)).alias("losses"),
        F.sum(F.when(F.col("pf") == F.col("pa"), 1).otherwise(0)).alias("ties"),
        F.sum("pf").alias("points_for"),
        F.sum("pa").alias("points_against"),
    )
    .withColumn("point_diff", F.col("points_for") - F.col("points_against"))
    .join(spark.table("silver_teams").select(F.col("team_abbr").alias("team"), "team_name",
                                             "team_conf", "team_division"), "team", "left")
)
records.write.format("delta").mode("overwrite").saveAsTable("gold_team_records")


# ======================= CELL 7: maintenance + a peek =======================
for t in ["silver_player_week", "gold_qb_season", "gold_fantasy_leaders", "gold_team_records"]:
    spark.sql(f"OPTIMIZE {t}")     # compacts small files (Fabric also applies V-Order for fast reads)

display(spark.sql("""
    SELECT player_name, team, passing_yards, passing_tds, epa_per_dropback
    FROM gold_qb_season
    WHERE season = 2024
    ORDER BY epa_per_dropback DESC
    LIMIT 10
"""))

display(spark.sql("DESCRIBE HISTORY silver_player_week").select("version", "timestamp", "operation"))
