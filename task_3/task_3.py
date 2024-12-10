import pickle
import sqlite3
import json


def load_pkl(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def load_txt(path):
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read()
    items = []
    for row in text.split('=====\n'):
        if row == '':
            continue
        item = {}
        for field in row.split('\n'):
            if field == '':
                continue
            k, v = field.split('::')
            item[k] = v
        items.append(item)
    return items


def connect_to_db(path):
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def create_songs_table(db):
    cursor = db.cursor()
    cursor.execute("""
        create table songs (
            id integer not null unique primary key autoincrement,
            artist text,
            song text,
            duration_ms integer,
            year integer,
            tempo real,
            genre text
        )
    """)
    db.commit()


def insert_songs_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        insert into songs (artist, song, duration_ms, year, tempo, genre)
        values (:artist, :song, :duration_ms, :year, :tempo, :genre)
    """, data)
    db.commit()


def write_to_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select *
        from songs
        order by duration_ms desc
        limit 15
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_1.json', items)


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select sum(tempo) as sum_tempo, min(tempo) as min_tempo, max(tempo) as max_tempo, round(avg(tempo), 2) as avg_tempo
        from songs
    """)
    write_to_json('query_2.json', dict(result.fetchone()))


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select year, count(*) as count
        from songs
        group by year
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_3.json', items)


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select *
        from songs
        where genre = 'hip hop'
        order by tempo desc
        limit 20
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_4.json', items)


db = connect_to_db('../task_3.db')

#STEP: insert data
# data_part_1 = load_txt('../data/3/_part_1.text')
# data_part_2 = load_pkl('../data/3/_part_2.pkl')
# create_songs_table(db)
# insert_songs_data(db, data_part_1)
# insert_songs_data(db, data_part_2)

#STEP: queries
first_query(db)
second_query(db)
third_query(db)
fourth_query(db)