# DROP TABLES
games_table_drop = "DROP TABLE IF EXISTS games"
champions_table_drop = "DROP TABLE IF EXISTS champions"
items_table_drop = "DROP TABLE IF EXISTS items"
objectives_visions_table_drop = "DROP TABLE IF EXISTS objectives_visions"
champion_key_table_drop = "DROP TABLE IF EXISTS champion_key"
item_key_table_drop = "DROP TABLE IF EXISTS item_key"

games_table_create = ("""
CREATE TABLE IF NOT EXISTS games(
    game_id bigint PRIMARY KEY,
    game_duration float NOT NULL, 
    game_version varchar NOT NULL, 
    participants varchar[10] NOT NULL)
    """)
champions_table_create = ("""
CREATE TABLE IF NOT EXISTS champions(
    game_id bigint PRIMARY KEY, champ_1 int NOT NULL, 
    champ_2 int NOT NULL, 
    champ_3 int NOT NULL, 
    champ_4 int NOT NULL, 
    champ_5 int NOT NULL, 
    champ_6 int NOT NULL, 
    champ_7 int NOT NULL, 
    champ_8 int NOT NULL, 
    champ_9 int NOT NULL, 
    champ_10 int NOT NULL)
    """)
items_table_create = ("""
CREATE TABLE IF NOT EXISTS items(
    game_id bigint PRIMARY KEY, 
    build_1 int[6] NOT NULL, 
    build_2 int[6] NOT NULL, 
    build_3 int[6] NOT NULL, 
    build_4 int[6] NOT NULL, 
    build_5 int[6] NOT NULL, 
    build_6 int[6] NOT NULL, 
    build_7 int[6] NOT NULL, 
    build_8 int[6] NOT NULL, 
    build_9 int[6] NOT NULL, 
    build_10 int[6] NOT NULL)
    """)
objectives_visions_table_create = ("""
CREATE TABLE IF NOT EXISTS objectives_visions(
    game_id bigint PRIMARY KEY, 
    win_dragon_soul boolean NOT NULL, 
    win_baron_nashor boolean NOT NULL, 
    win_ward_placed int NOT NULL, 
    win_ward_destroyed int NOT NULL, 
    lose_dragon_soul boolean NOT NULL, 
    lose_baron_nashor boolean NOT NULL, 
    lose_ward_placed int NOT NULL, 
    lose_ward_destroyed int NOT NULL)
    """)
champion_key_table_create = ("""
CREATE TABLE IF NOT EXISTS champion_key(
    champion_key bigint PRIMARY KEY, 
    champion_name varchar NOT NULL)
    """)
item_key_table_create = ("""
CREATE TABLE IF NOT EXISTS item_key(
    item_key bigint PRIMARY KEY, 
    item_name varchar NOT NULL)
    """)
champions_table_insert = ("""
INSERT INTO champions(game_id, champ_1, champ_2, champ_3, champ_4, champ_5, champ_6, champ_7, champ_8, champ_9, champ_10)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(game_id) DO NOTHING;
""")
items_table_insert = ("""
INSERT INTO items(game_id, build_1, build_2, build_3, build_4, build_5, build_6, build_7, build_8, build_9, build_10)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(game_id) DO NOTHING;
""")
objectives_visions_table_insert = ("""
INSERT INTO objectives_visions(game_id, lose_ward_placed, win_ward_placed, lose_ward_destroyed, win_ward_destroyed, win_baron_nashor, win_dragon_soul, lose_baron_nashor, lose_dragon_soul) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(game_id) DO NOTHING;
""")
champion_key_table_insert = ("""
INSERT INTO champion_key(champion_key, champion_name) 
VALUES (%s, %s)
ON CONFLICT(champion_key) DO NOTHING;
""")
item_key_table_insert = ("""
INSERT INTO item_key(item_name, item_key)
VALUES (%s, %s)
ON CONFLICT(item_key) DO NOTHING;
""")
games_table_insert = ("""
INSERT INTO games(game_id, participants, game_duration, game_version) 
VALUES (%s, %s,%s, %s)
ON CONFLICT(game_id) DO NOTHING;
""")


drop_table_queries = [games_table_drop, champions_table_drop, items_table_drop, objectives_visions_table_drop, champion_key_table_drop, item_key_table_drop]
create_table_queries = [games_table_create, champions_table_create, items_table_create, objectives_visions_table_create, champion_key_table_create, item_key_table_create]
iserntion = [champions_table_insert]

