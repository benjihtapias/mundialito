import pandas as pd
import pyodbc
import re
import os
import json
import glob

with open("../../../config.json", "r") as f:
    config = json.load(f)

MUND_ID = config["mundialitoId"]
input_path = config["inputPath"]
out_path = config["outputPath"]

search_pattern = os.path.join(input_path, f"mundialito_{MUND_ID}*.csv")
matching_files = glob.glob(search_pattern)

if len(matching_files) == 0:
    raise FileNotFoundError(f"No file found matching: {search_pattern}")

if len(matching_files) > 1:
    raise RuntimeError(f"Multiple files found matching: {search_pattern}\nFiles: {matching_files}")

file_path = matching_files[0]

basename = os.path.basename(file_path).replace(".csv", "_STAGING.csv")
output_csv = os.path.join(out_path, basename)

print("Input file found:", file_path)
print("Output staging file:", output_csv)

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={config['server']};"
    f"DATABASE={config['database']};"
    "Trusted_Connection=yes;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

MUND_STR = str.split(file_path, "/")[-1]
MUND_NAME = str.split(MUND_STR, "_")[0].capitalize() + " " + str(MUND_ID)
MUND_DATE = str.split(str.split(MUND_STR, "_")[2], ".")[0]

df = pd.read_csv(file_path, index_col=False)
df = df.drop(columns=df.columns[df.columns.str.startswith("Unnamed")])

# DATA VALIDATION

"""""
NEW HEADERS

EXPECTED_COLUMNS = [
    "Name", "TeamName", "TeamAbbr",
    "OppName", "OppAbbr",
    "WL", "Goals", "Assists",
    "GoalsConceded", "Game", "MVP"
]
"""

# OLD HEADERS
EXPECTED_COLUMNS = [
    "Name", "TeamName", "TeamAbbr", "GamesPlayed", "GamesWon", "GamesLost", "Goals", "Assists", "MVP"
]
print(df.columns)
assert list(df.columns) == EXPECTED_COLUMNS

"""""
NEW DATA VALIDATION

# Win loss input check

assert df["WL"].isin([0, 1]).all()

# Total goals scored == Total goals conceded

for game_id, g in df.groupby("Game"):
    total_scored = g["Goals"].sum()
    total_conceded = g["GoalsConceded"].sum()
    assert total_scored == total_conceded, f"Mismatch in Game {game_id}"


"""

#df["CleanSheet"] = (df["GoalsConceded"] == 0).astype(int)

# OLD DATA STATS

# existing players
cursor.execute("SELECT PlayerId, Name FROM Players;")
existing_players = cursor.fetchall()

name_to_existing_player_id = {row.Name: row.PlayerId for row in existing_players}

cursor.execute("SELECT ISNULL(MAX(PlayerId), 0) FROM Players;")
max_player_id = cursor.fetchone()[0]

print(f"Existing players: {len(name_to_existing_player_id)}, max PlayerId in DB: {max_player_id}")

# existing teams (only used when pushing to database a second time for the same tourney)
cursor.execute("""
    SELECT TeamId, TeamName, TeamAbbr, MundialitoId
    FROM Teams;
""")
existing_teams = cursor.fetchall()

existing_team_key_to_id = {
    (row.TeamName, row.TeamAbbr, row.MundialitoId): row.TeamId
    for row in existing_teams
}

print(f"Existing teams: {len(existing_team_key_to_id)} total")

cursor.execute("SELECT ISNULL(MAX(TeamId), 0) FROM Teams;")
max_team_id = cursor.fetchone()[0]
print(f"Max TeamId in DB: {max_team_id}")

player_name_to_id = dict(name_to_existing_player_id)
player_name_is_new = {name: False for name in name_to_existing_player_id}

for name in df["Name"].unique():
    if name not in player_name_to_id:
        max_player_id += 1
        player_name_to_id[name] = max_player_id
        player_name_is_new[name] = True

team_key_to_id = {}
team_key_is_new = {}

unique_teams = df[["TeamName", "TeamAbbr"]].drop_duplicates()

for _, row in unique_teams.iterrows():
    team_name = row["TeamName"]
    team_abbr = row["TeamAbbr"]
    key = (team_name, team_abbr, MUND_ID)

    if key in existing_team_key_to_id:
        team_id = existing_team_key_to_id[key]
        is_new = False
    else:
        max_team_id += 1
        team_id = max_team_id
        is_new = True

    team_key_to_id[key] = team_id
    team_key_is_new[key] = is_new

print(f"Total players in staging: {len(player_name_to_id)}")
print(f"Total teams in staging for Mundialito {MUND_ID}: {len(team_key_to_id)}")

max_goals = df["Goals"].max()
max_assists = df["Assists"].max()

golden_boot_keys = set(df.loc[df["Goals"] == max_goals, "Name"])
playmaker_keys = set(df.loc[df["Assists"] == max_assists, "Name"])
mvp_keys = set(df.loc[df["MVP"] == 1, "Name"])

print(golden_boot_keys)
print(playmaker_keys)

rows = []

for _, row in df.iterrows():
    name = row["Name"]
    team_name = row["TeamName"]
    team_abbr = row["TeamAbbr"]
    key = (team_name, team_abbr, MUND_ID)

    player_id = player_name_to_id[name]
    team_id = team_key_to_id[key]

    is_mvp = name in mvp_keys
    is_golden_boot = name in golden_boot_keys
    is_playmaker = name in playmaker_keys

    rows.append({
        "PlayerId": player_id,
        "MundialitoId": MUND_ID,
        "TeamId": team_id,
        "GamesPlayed": int(row["GamesPlayed"]),
        "GamesWon": int(row["GamesWon"]),
        "GamesLost": int(row["GamesLost"]),
        "Goals": int(row["Goals"]),
        "Assists": int(row["Assists"]),
        "CleanSheets": None,       # old data didn't track this
        "GoalsConceded": None,     # old data didn't track this

        "PlayerName": name,
        "TeamName": team_name,
        "TeamAbbr": team_abbr,
        "IsNewPlayer": player_name_is_new.get(name, False),
        "IsNewTeam": team_key_is_new[key],

        "IsMVP": is_mvp,
        "IsGoldenBoot": is_golden_boot,
        "IsPlaymaker": is_playmaker,
    })

staging_df = pd.DataFrame(rows)

staging_df = staging_df.sort_values(
    ["TeamId", "PlayerName"]
).reset_index(drop=True)

staging_df.to_csv(output_csv, index=False)
print(f"\nStaging CSV created (no DB modifications):\n{output_csv}")