import pandas as pd
import pyodbc
import os
import glob
import json

with open("../../../config.json", "r") as f:
    config = json.load(f)

MUND_ID = config["mundialitoId"] 
out_path = config["outputPath"]

search_pattern = os.path.join(out_path, f"mundialito_{MUND_ID}*.csv")
matching_files = glob.glob(search_pattern)

if len(matching_files) == 0:
    raise FileNotFoundError(f"No file found matching: {search_pattern}")

if len(matching_files) > 1:
    raise RuntimeError(f"Multiple files found matching: {search_pattern}\nFiles: {matching_files}")

staging_csv = matching_files[0]
MUND_DATE = str.split(str.split(staging_csv, "_")[3], ".")[0]

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={config['server']};"
    f"DATABASE={config['database']};"
    "Trusted_Connection=yes;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

conn.autocommit = False

df = pd.read_csv(staging_csv)

required_columns = [
    "PlayerId", "MundialitoId", "TeamId",
    "GamesPlayed", "GamesWon", "GamesLost",
    "Goals", "Assists", "CleanSheets", "GoalsConceded",
    "PlayerName", "TeamName", "TeamAbbr",
    "IsNewPlayer", "IsNewTeam"
]

missing = [c for c in required_columns if c not in df.columns]
if missing:
    raise ValueError(f"Staging CSV missing required columns: {missing}")

mundialito_ids = df["MundialitoId"].unique()
if len(mundialito_ids) != 1:
    raise ValueError(f"Expected exactly 1 MundialitoId in staging file, found: {mundialito_ids}")

mundialito_id = int(mundialito_ids[0])
print(f"Importing for MundialitoId = {mundialito_id}")

df = df.where(pd.notnull(df), None)

try:

    cursor.execute("SELECT MundialitoId FROM Mundialitos WHERE MundialitoId = ?", mundialito_id)
    row = cursor.fetchone()

    if not row:
        print(f"MundialitoId {mundialito_id} not found in DB, creating it...")
        mundialito_name = f"Mundialito {mundialito_id}"

        cursor.execute("SET IDENTITY_INSERT Mundialitos ON;")
        cursor.execute(
            """
            INSERT INTO Mundialitos (MundialitoId, Name, Date)
            VALUES (?, ?, ?)
            """,
            mundialito_id, mundialito_name, MUND_DATE
        )
        cursor.execute("SET IDENTITY_INSERT Mundialitos OFF;")
    else:
        print(f"MundialitoId {mundialito_id} already exists.")


    new_players = (
        df[df["IsNewPlayer"] == True][["PlayerId", "PlayerName"]]
        .drop_duplicates(subset=["PlayerId"])
    )

    if not new_players.empty:
        print(f"Inserting {len(new_players)} new players...")
        cursor.execute("SET IDENTITY_INSERT Players ON;")

        for _, row in new_players.iterrows():
            player_id = int(row["PlayerId"])
            name = row["PlayerName"]
            cursor.execute(
                "INSERT INTO Players (PlayerId, Name) VALUES (?, ?)",
                player_id, name
            )

        cursor.execute("SET IDENTITY_INSERT Players OFF;")
    else:
        print("No new players to insert.")

    new_teams = (
        df[df["IsNewTeam"] == True][["TeamId", "TeamName", "TeamAbbr", "MundialitoId"]]
        .drop_duplicates(subset=["TeamId"])
    )

    if not new_teams.empty:
        print(f"Inserting {len(new_teams)} new teams for Mundialito {mundialito_id}...")
        cursor.execute("SET IDENTITY_INSERT Teams ON;")

        for _, row in new_teams.iterrows():
            team_id = int(row["TeamId"])
            team_name = row["TeamName"]
            team_abbr = row["TeamAbbr"]
            mid = int(row["MundialitoId"])

            cursor.execute(
                """
                INSERT INTO Teams (TeamId, MundialitoId, TeamName, TeamAbbr)
                VALUES (?, ?, ?, ?)
                """,
                team_id, mid, team_name, team_abbr
            )

        cursor.execute("SET IDENTITY_INSERT Teams OFF;")
    else:
        print("No new teams to insert.")

    print(f"Deleting existing PlayerTournamentStats for MundialitoId = {mundialito_id}...")
    cursor.execute(
        "DELETE FROM PlayerTournamentStats WHERE MundialitoId = ?",
        mundialito_id
    )

    pts_columns = [
        "PlayerId", "MundialitoId", "TeamId",
        "GamesPlayed", "GamesWon", "GamesLost",
        "Goals", "Assists", "CleanSheets", "GoalsConceded"
    ]

    print(f"Inserting {len(df)} PlayerTournamentStats rows...")
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO PlayerTournamentStats
                (PlayerId, MundialitoId, TeamId,
                 GamesPlayed, GamesWon, GamesLost,
                 Goals, Assists, CleanSheets, GoalsConceded)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            int(row["PlayerId"]),
            int(row["MundialitoId"]),
            int(row["TeamId"]),
            int(row["GamesPlayed"]),
            int(row["GamesWon"]),
            int(row["GamesLost"]),
            int(row["Goals"]),
            int(row["Assists"]),
            None, # int(row["CleanSheets"]),    change later for mundialito 4
            None # int(row["GoalsConceded"])
        )

    print(f"Deleting existing PlayerAwards for MundialitoId = {mundialito_id}...")
    cursor.execute(
        "DELETE FROM PlayerAwards WHERE MundialitoId = ?",
        mundialito_id
    )

    print(f"Inserting {len(df)} PlayerAwards rows...")
    for _, row in df.iterrows():

        player_id = int(row["PlayerId"])
        mid = int(row["MundialitoId"])

        is_mvp = bool(row["IsMVP"])
        is_gb = bool(row["IsGoldenBoot"])
        is_pm = bool(row["IsPlaymaker"])

        cursor.execute(
            """
            INSERT INTO PlayerAwards
               (PlayerId, MundialitoId, IsMVP, IsGoldenBoot, IsPlaymaker)
            VALUES (?, ?, ?, ?, ?)
            """,
            player_id,
            mid,
            1 if is_mvp else 0,
            1 if is_gb else 0,
            1 if is_pm else 0
        )

    print("Deleting existing TeamTournamentStats...")
    cursor.execute(
        "DELETE FROM TeamTournamentStats WHERE MundialitoId = ?",
        mundialito_id
    )

    team_group = df.groupby(
        ["TeamId", "TeamName", "TeamAbbr", "MundialitoId"],
        as_index=False
    ).agg({
        "GamesPlayed": "max",
        "GamesWon": "max",
        "GamesLost": "max",
        "Goals": "sum",
        "Assists": "sum",
        "GoalsConceded": "sum",
        "CleanSheets": "sum",
    })

    max_wins = team_group["GamesWon"].max()
    team_group["IsChampion"] = team_group["GamesWon"].apply(
        lambda w: 1 if w == max_wins else 0
    )

    print(f"Inserting {len(team_group)} TeamTournamentStats rows...")

    for _, row in team_group.iterrows():
        cursor.execute(
            """
            INSERT INTO TeamTournamentStats
                (TeamId, MundialitoId,
                GamesPlayed, GamesWon, GamesLost,
                Goals, Assists, GoalsConceded, CleanSheets, IsChampion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            int(row["TeamId"]),
            int(row["MundialitoId"]),
            int(row["GamesPlayed"]),
            int(row["GamesWon"]),
            int(row["GamesLost"]),
            int(row["Goals"]),
            int(row["Assists"]),
            None,
            None,
            int(row["IsChampion"])
        )

    conn.commit()
    print("Import completed successfully.")

except Exception as e:
    print("ERROR during import, rolling back...")
    conn.rollback()
    raise

finally:
    cursor.close()
    conn.close()
