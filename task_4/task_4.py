import csv
import pickle
import sqlite3
import json


def load_pkl(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def load_csv(path):
    extracted_csv = []
    with open(path, newline='', encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            if row['views'] is None: # у некоторых строк пустая category, но не хватает ';' - обрабатываю такие случаи
                row['views'] = row['isAvailable']
                row['isAvailable'] = row['fromCity']
                row['fromCity'] = row['category']
                row['category'] = None
            row['isAvailable'] = row['isAvailable'] == 'True'
            extracted_csv.append(row)

    return extracted_csv


def connect_to_db(path):
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def create_products_table(db):
    cursor = db.cursor()
    cursor.execute("""
        create table products (
            id integer not null unique primary key autoincrement,
            name text,
            price real,
            quantity integer,
            category text,
            fromCity text,
            isAvailable integer,
            views integer,
            version integer not null default 0,
            check (price >= 0),
            check (quantity >= 0)
        )
    """)
    db.commit()


def insert_products_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        insert into products (name, price, quantity, category, fromCity, isAvailable, views)
        values (:name, :price, :quantity, :category, :fromCity, :isAvailable, :views)
    """, data)
    db.commit()

def handle_quantity_change(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        update products
        set quantity = quantity + ?,
        version = version + 1
        where name = ?
    """, [param, name])
    db.commit()


def handle_remove(db, name):
    cursor = db.cursor()
    cursor.execute("""
        delete from products
        where name = ?
    """, [name])
    db.commit()


def handle_price_percent(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        update products
        set price = round(price * (1 + ?), 3),
        version = version + 1
        where name = ?
    """, [param, name])
    db.commit()


def handle_price_abs(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        update products
        set price = round(price + ?, 3),
        version = version + 1
        where name = ?
    """, [param, name])
    db.commit()


def handle_available(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        update products
        set isAvailable = ?,
        version = version + 1
        where name = ?
    """, [param, name])
    db.commit()


def handle_updates(db, updates_data):
    for update in updates_data:
        try:
            if update['method'] == 'quantity_add' or update['method'] == 'quantity_sub':
                handle_quantity_change(db, update['name'], update['param'])
            elif update['method'] == 'remove':
                handle_remove(db, update['name'])
            if update['method'] == 'price_percent':
                handle_price_percent(db, update['name'], update['param'])
            if update['method'] == 'price_abs':
                handle_price_abs(db, update['name'], update['param'])
            if update['method'] == 'available':
                handle_available(db, update['name'], update['param'])
        except sqlite3.IntegrityError:
            print(f"Trying to apply illegal update: {update}")


def write_to_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select *
        from products
        order by version desc
        limit 10
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_1.json', items)


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select category, sum(price) as sum_price, min(price) as min_price, max(price) as max_price, round(avg(price), 2) as avg_price, count(*) as count
        from products
        group by category
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_2.json', items)


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select category, sum(quantity) as sum_quantity, min(quantity) as min_quantity, max(quantity) as max_quantity, round(avg(quantity), 2) as avg_quantity
        from products
        group by category
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_3.json', items)


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        select fromCity, round(avg(price), 2) as avg_price, count(*) as count
        from products
        group by fromCity
        order by count(*) desc
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    write_to_json('query_4.json', items)


db = connect_to_db('../task_4.db')

#STEP: insert data
# data = load_csv('../data/4/_product_data.csv') # проверил, что дубликатов строк нет
# create_products_table(db)
# insert_products_data(db, data)
#
# #STEP: update data
# update_data = load_pkl('../data/4/_update_data.pkl')
# handle_updates(db, update_data)

#STEP: queries
first_query(db)
second_query(db)
third_query(db)
fourth_query(db)