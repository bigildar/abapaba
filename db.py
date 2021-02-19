import sqlite3
import random



def ensure_connection(func):
    """ Декоратор для подключения к СУБД: открывает соединение,
        выполняет переданную функцию и закрывает за собой соединение.
        Потокобезопасно!
    """
    def inner(*args, **kwargs):
        with sqlite3.connect('abap.db') as conn:
            kwargs['conn'] = conn
            res = func(*args, **kwargs)
        return res
    return inner


@ensure_connection
def init_db(conn, force: bool = False):
    c = conn.cursor()

    if force:
        c.execute('DROP TABLE IF EXISTS warehause')
        c.execute('DROP TABLE IF EXISTS goods')
        c.execute('DROP TABLE IF EXISTS product')
        c.execute('DROP TABLE IF EXISTS brand')
    c.execute('''
        CREATE TABLE IF NOT EXISTS warehause (
            id         INTEGER PRIMARY KEY,
            name       TEXT NOT NULL
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS goods (
            id          INTEGER PRIMARY KEY,
            warehauseId       INT,
            productId         INT,
            quantity          INT,
            FOREIGN KEY (warehauseId) REFERENCES warehause(id),
            FOREIGN KEY (productId) REFERENCES product(id)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS product (
            id          INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            brandId     INT,
            FOREIGN KEY (brandId) REFERENCES brand(id)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS brand (
            id          INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            country     TEXT NOT NULL
        )
    ''')
    # Сохранить изменения
    conn.commit()

@ensure_connection  
def add_record(conn, brand: str, country: str, name: str, warename: str, quantity=None):
    warehauseId=add_warehause(warename=warename)   
    brandId=add_brand(brand=brand, country=country)
    productId=add_product(name=name, brandId=brandId)
    add_goods(warehauseId=warehauseId, productId=productId, quantity=quantity)

@ensure_connection  
def add_warehause(conn, warename: str):
    c = conn.cursor()
    c.execute('SELECT EXISTS(SELECT id FROM warehause WHERE name = ?)', (warename,))
    (f,)=c.fetchone()
    if f==False:
        c.execute('INSERT INTO warehause (name) VALUES (?)', (warename,))
        warehauseId=c.lastrowid
    else:
        c.execute('SELECT id FROM warehause WHERE name = ?', (warename,))
        (warehauseId,)=c.fetchone()
    conn.commit()
    return warehauseId

@ensure_connection
def add_brand(conn, brand:str, country: str):
    c = conn.cursor()
    c.execute('SELECT EXISTS(SELECT id FROM brand WHERE name = ? AND country=?)', (brand, country))
    (f,)=c.fetchone()
    if f==False:
        c.execute('INSERT INTO brand (name, country) VALUES (?,?)', (brand, country))
        brandId=c.lastrowid
    else:
        c.execute('SELECT id FROM brand WHERE name = ? AND country=?', (brand, country))
        (brandId,)=c.fetchone()
    conn.commit()
    return brandId

@ensure_connection
def add_product(conn, name:str, brandId: int):
    c = conn.cursor()
    c.execute('SELECT EXISTS(SELECT id FROM product WHERE name = ? AND brandId=?)', (name, brandId))
    (f,)=c.fetchone()
    if f==False:
        c.execute('INSERT INTO product (name, brandId) VALUES (?,?)', (name, brandId))
        brandId=c.lastrowid
    else:
        c.execute('SELECT id FROM product WHERE name = ? AND brandId=?', (name, brandId))
        (brandId,)=c.fetchone()
    conn.commit()
    return brandId

@ensure_connection
def add_goods(conn, warehauseId: int, productId: int, quantity: int):
    c = conn.cursor()
    c.execute('SELECT EXISTS(SELECT id FROM goods WHERE warehauseId = ? AND productId=?)', (warehauseId, productId))
    (f,)=c.fetchone()
    if f==False:
        if quantity==0: quantity=None
        c.execute('INSERT INTO goods (warehauseId, productId, quantity) VALUES (?,?,?)', (warehauseId, productId, quantity))
    else:
        c.execute('SELECT quantity FROM goods WHERE warehauseId = ? AND productId=?', (warehauseId, productId))
        (quantity_bd,)=c.fetchone()
        if quantity_bd==None: quantity_bd=0
        quantity_bd += quantity
        if quantity_bd==0: quantity_bd=None
        c.execute('UPDATE goods SET quantity = ? WHERE warehauseId = ? AND productId=?', (quantity_bd, warehauseId, productId))
    conn.commit()

@ensure_connection
def get_table(conn):
    c = conn.cursor()
    c.execute('''
        select brand.name, product.name, goods.quantity, warehause.name
        from brand, product, goods, warehause
        where product.brandId = brand.id and goods.productId = product.id and goods.warehauseId = warehause.id
        order by brand.name, goods.quantity desc
        ''')
    f=c.fetchall()
    for i in f:
        print(i)

@ensure_connection
def get_brand_balance(conn, country: str):
    c = conn.cursor()
    c.execute('''
        select warehause.name, sum(goods.quantity)
        from brand, product, goods, warehause
        where product.brandId = brand.id and goods.productId = product.id and 
        goods.quantity is not null and goods.warehauseId = warehause.id and brand.country = ?
        group by warehause.name
        order by warehause.name
        ''',(country,))
    f=c.fetchall()
    for i in f:
        print(i)


@ensure_connection
def get_quantity_none(conn,):
    c = conn.cursor()
    c.execute('''
        select brand.name, product.name
        from brand, product, goods, warehause
        where product.brandId = brand.id and goods.productId = product.id and 
        goods.warehauseId = warehause.id and goods.quantity is null
        except
        select brand.name, product.name
        from brand, product, goods, warehause
        where product.brandId = brand.id and goods.productId = product.id and 
        goods.warehauseId = warehause.id and goods.quantity is not null
        ''')
    f=c.fetchall()
    for i in f:
        print(i)

if __name__ == '__main__':
    '''
    Тест
    '''
    from random import choice 
    from random import randint
    
    brands=['Газпром','Лукойл', 'Пятерочка','Магнит','ММК','Лада',
            'Apple','Мишлен','Amazon','Tesla','Toyota','BMW']
    countries=['Россия', 'США', 'Германия']
    products=['Масло','Телефон','Двигатель','Доски','Хлеб','Мука',
             'Рельса','Молоко','Уголь','Ложка','Антенна','Бумага']
    warehauses=['Москва', 'Санкт-Петербург', 'Челябниск', 'Краснодар']


    init_db(force=True)

    for i in range(10):
        add_record(
            brand=choice(brands), 
            country=choice(countries), 
            name=choice(products), 
            warename=choice(warehauses), 
            quantity=randint(0,2)*100)

    get_table()
    print('*******************')
    get_brand_balance(country='США')
    print('****')
    get_brand_balance(country='Россия')
    print('*****')
    get_brand_balance(country='Германия')
    print('*******************')
    get_quantity_none()

