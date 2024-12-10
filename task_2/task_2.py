import pickle
import sqlite3
import json


def load_pkl(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def connect_to_db(path):
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def create_book_sale_lots_table(db):
    cursor = db.cursor()
    cursor.execute("""
        create table book_sale_lots (
            id integer not null unique primary key autoincrement,
            book_title text references books(title),
            price integer,
            place text,
            date text
        )
    """)
    db.commit()


def insert_book_sale_lots_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        insert into book_sale_lots (book_title, price, place, date)
        values (:title, :price, :place, :date)
    """, data)
    db.commit()


def write_to_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select b.author as author, count(*) as book_sale_lots
        from books b
            join book_sale_lots bsl
            on b.title = bsl.book_title
        group by b.author
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_1.json', items)


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select b.genre as genre, round(avg(bsl.price), 2) as avg_book_price
        from books b
            join book_sale_lots bsl
            on b.title = bsl.book_title
        group by b.genre
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_2.json', items)


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select b.id, b.title, b.author, b.genre, max(bsl.price) as max_online_price
        from books b
            join book_sale_lots bsl
            on b.title = bsl.book_title
        where bsl.place = 'online'
        group by b.id, b.title, b.author, b.genre
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_3.json', items)


db = connect_to_db('../task_1-2.db')

#STEP: insert data
# data = load_pkl('../data/1-2/subitem.pkl')
# create_book_sale_lots_table(db)
# insert_book_sale_lots_data(db, data)

# #STEP: queries
first_query(db)
second_query(db)
third_query(db)