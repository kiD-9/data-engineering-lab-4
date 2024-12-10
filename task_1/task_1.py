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


def create_books_table(db):
    cursor = db.cursor()
    cursor.execute("""
        create table books (
            id integer not null unique primary key autoincrement,
            title text,
            author text,
            genre text,
            isbn text,
            pages integer,
            published_year integer,
            views integer,
            rating real
        )
    """)
    db.commit()


def insert_books_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        insert into books (title, author, genre, isbn, pages, published_year, views, rating)
        values (:title, :author, :genre, :isbn, :pages, :published_year, :views, :rating)
    """, data)
    db.commit()


def write_to_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select *
        from books
        order by rating desc
        limit 15
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_1.json', items)


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select sum(pages) as sum_pages, min(pages) as min_pages, max(pages) as max_pages, round(avg(pages), 2) as avg_pages
        from books
    """)
    write_to_json('query_2.json', dict(result.fetchone()))


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select genre, count(*) as count
        from books
        group by genre
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_3.json', items)


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select *
        from books
        where genre = 'детектив'
        order by published_year desc
        limit 15
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_4.json', items)


db = connect_to_db('../task_1-2.db')

#STEP: insert data
# data = load_pkl('../data/1-2/item.pkl')
# create_books_table(db)
# insert_books_data(db, data)

#STEP: queries
first_query(db)
second_query(db)
third_query(db)
fourth_query(db)