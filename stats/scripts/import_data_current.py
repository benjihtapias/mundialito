import pandas as pd
import pyodbc
import os
import glob
import json

with open("../config.json", "r") as f:
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

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=Mundialito;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

conn.autocommit = False

df = pd.read_csv(staging_csv)

required_columns = [
    "PlayerId", 
    "MundialitoId", 
    "TeamId",
    "OppId", 
    "Game", 
    "WL",
    "Pts",
    "Goals", 
    "Assists", 
    "CleanSheet", 
    "GoalsConceded",
    "GameId",
    "GameIdStr",
    "PlayerName", 
    "TeamName", 
    "TeamAbbr",
    "OppName", 
    "OppAbbr",
    "IsNewPlayer", 
    "IsNewTeam",
    "IsMVP",
    "IsGoldenBoot",
    "IsPlaymaker"
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

    # ------ INSERTING MUNDIALITOS -------

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

    # ------ INSERTING PLAYERS -------

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

    # ------ INSERTING TEAMS -------

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
                INSERT INTO Teams (TeamId, MundialitoId, TeamName, TeamAbbreviation)
                VALUES (?, ?, ?, ?)
                """,
                team_id, mid, team_name, team_abbr
            )

        cursor.execute("SET IDENTITY_INSERT Teams OFF;")
    else:
        print("No new teams to insert.")


    # ------ INSERTING GAMES -------

    print(f"Deleting existing Games for MundialitoId = {mundialito_id}...")
    cursor.execute(
        "DELETE FROM Games WHERE MundialitoId = ?",
        mundialito_id
    )

    games_df = (
        df.assign(
            TeamAId=lambda x: x[["TeamId", "OppId"]].min(axis=1),
            TeamBId=lambda x: x[["TeamId", "OppId"]].max(axis=1),
        )
        .assign(
            GoalsA=lambda x: x["Goals"].where(x["TeamId"] == x["TeamAId"], 0),
            GoalsB=lambda x: x["GoalsConceded"].where(x["TeamId"] == x["TeamAId"], 0),
        )
        .groupby(["GameId", "MundialitoId", "TeamAId", "TeamBId"], as_index=False)
        .agg(
            GoalsA=("GoalsA", "sum"),
            GoalsB=("GoalsB", "max"),
        )
    )

    print(games_df)

    print(f"Inserting {len(games_df)} Games rows...")
    for _, row in games_df.iterrows():
        cursor.execute(
            """
            INSERT INTO Games
                (GameId, MundialitoId, TeamAId, TeamBId, GoalsA, GoalsB)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            str(row["GameId"]),
            int(row["MundialitoId"]),
            int(row["TeamAId"]),
            int(row["TeamBId"]),
            int(row["GoalsA"]),
            int(row["GoalsB"])
        )

    # ------ INSERTING PLAYERGAMESTATS -------

    print(f"Deleting existing PlayerGameStats for MundialitoId = {mundialito_id}...")
    cursor.execute(
        "DELETE FROM PlayerGameStats WHERE MundialitoId = ?",
        mundialito_id
    )

    print(f"Inserting {len(df)} PlayerGameStats rows...")
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO PlayerGameStats
                (GameId, MundialitoId, PlayerId, TeamId,
                OppTeamId, WL, Goals,
                Assists, GoalsConceded)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            str(row["GameId"]),
            int(row["MundialitoId"]),
            int(row["PlayerId"]),
            int(row["TeamId"]),  
            int(row["OppId"]),
            str(row["WL"]),
            int(row["Goals"]),
            int(row["Assists"]),
            int(row["GoalsConceded"])
        )

    # ------ INSERTING PLAYERTOURNAMENTSTATS -------

    print(f"Deleting existing PlayerTournamentStats for MundialitoId = {mundialito_id}...")
    cursor.execute(
        "DELETE FROM PlayerTournamentStats WHERE MundialitoId = ?",
        mundialito_id
    )

    pts_df = df.groupby(
        ["PlayerId", "TeamName", "TeamId", "TeamAbbr", "MundialitoId"],
        as_index=False
    ).agg(
        GamesPlayed=("WL", "count"),
        GamesWon=("WL", lambda x: (x == "W").sum()),
        GamesDrawn=("WL", lambda x: (x == "D").sum()),
        GamesLost=("WL", lambda x: (x == "L").sum()),
        Goals=("Goals", "sum"),
        Assists=("Assists", "sum"),
        GoalsConceded=("GoalsConceded", "sum"),
        CleanSheets=("CleanSheet", "sum"),
    )

    print(pts_df)

    print(f"Inserting {len(df)} PlayerTournamentStats rows...")
    for _, row in pts_df.iterrows():
        cursor.execute(
            """
            INSERT INTO PlayerTournamentStats
                (PlayerId, MundialitoId, TeamId,
                 GamesPlayed, GamesWon, GamesDrawn, GamesLost,
                 Goals, Assists, CleanSheets, GoalsConceded)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            int(row["PlayerId"]),
            int(row["MundialitoId"]),
            int(row["TeamId"]),
            int(row["GamesPlayed"]),
            int(row["GamesWon"]),
            int(row["GamesDrawn"]),
            int(row["GamesLost"]),
            int(row["Goals"]),
            int(row["Assists"]),
            int(row["CleanSheets"]),    
            int(row["GoalsConceded"])
        )

    # ------ INSERTING PLAYERAWARDS -------

    print(f"Deleting existing PlayerAwards for MundialitoId = {mundialito_id}...")
    cursor.execute(
        "DELETE FROM PlayerAwards WHERE MundialitoId = ?",
        mundialito_id
    )

    awards_df = (
        df.groupby(["PlayerId", "MundialitoId"], as_index=False)
          .agg({
              "IsMVP": "max",
              "IsGoldenBoot": "max",
              "IsPlaymaker": "max",
          })
    )

    print(f"Inserting {len(awards_df)} PlayerAwards rows...")
    for _, row in awards_df.iterrows():

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


    # ------ INSERTING TEAMTOURNAMENTSTATS -------

    print("Deleting existing TeamTournamentStats...")
    cursor.execute(
        "DELETE FROM TeamTournamentStats WHERE MundialitoId = ?",
        mundialito_id
    )

    tts_df = pts_df.groupby(
        ["TeamId", "MundialitoId"],
        as_index=False
    ).agg(
        GamesPlayed=("GamesPlayed", "max"),
        GamesWon=("GamesWon", "max"),
        GamesDrawn=("GamesDrawn", "max"),
        GamesLost=("GamesLost", "max"),
        Goals=("Goals", "sum"),
        Assists=("Assists", "sum"),
        GoalsConceded=("GoalsConceded", "max"),
        CleanSheets=("CleanSheets", "max"),
    )

    print(tts_df)

    max_wins = tts_df["GamesWon"].max()
    tts_df["IsChampion"] = tts_df["GamesWon"].apply(
        lambda w: 1 if w == max_wins else 0
    )

    print(f"Inserting {len(tts_df)} TeamTournamentStats rows...")

    for _, row in tts_df.iterrows():
        cursor.execute(
            """
            INSERT INTO TeamTournamentStats
                (TeamId, MundialitoId,
                GamesPlayed, GamesWon, GamesDrawn, GamesLost,
                Goals, Assists, GoalsConceded, CleanSheets, IsChampion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            int(row["TeamId"]),
            int(row["MundialitoId"]),
            int(row["GamesPlayed"]),
            int(row["GamesWon"]),
            int(row["GamesDrawn"]),
            int(row["GamesLost"]),
            int(row["Goals"]),
            int(row["Assists"]),
            int(row["GoalsConceded"]),
            int(row["CleanSheets"]),
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
