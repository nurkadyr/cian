import pyodbc


# Подключение к MS SQL Server

def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=192.168.1.59;"  # Имя сервера
        "DATABASE=ParsersDb2;"  # Имя базы данных
        "UID=nurkadyr;"  # Логин
        "PWD=rpxV3T1D"  # Пароль
        "TrustServerCertificate=yes"
    )

# Функция для добавления данных
def insert_product(conn,
                   source, category, segment_on_source, vehicle_sub_type, region, deal_type, type_,
                   brand, url, is_from_archive, task_guid, creation_date, status, mongo_id,
                   id_on_source, is_files_complete, last_modification_date, parser_version, weapon_kind, machine_name
                   ):
    query = """
    INSERT INTO Products (
        Source, Category, SegmentOnSource, VehicleSubType, Region, DealType, Type, Brand,
        Url, IsFromArchive, TaskGuid, CreationDate, Status, Mongo_Id, IdOnSource,
        IsFilesComplete, LastModificationDate, ParserVersion, WeaponKind, MachineName
    )
    OUTPUT INSERTED.Id
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (
            source, category, segment_on_source, vehicle_sub_type, region, deal_type, type_,
            brand, url, is_from_archive, task_guid, creation_date, status, mongo_id,
            id_on_source, is_files_complete, last_modification_date, parser_version, weapon_kind, machine_name
        ))
        inserted_id = cursor.fetchone()  # Проверяем, есть ли результат

        if inserted_id:  # Если `fetchone()` вернул что-то, берём `Id`
            inserted_id = inserted_id[0]
        else:
            inserted_id = None  # Если нет результата, устанавливаем `None`
        conn.commit()

    return inserted_id


def is_url_exists(conn, url: str) -> bool:
    """
    Проверяет, есть ли запись с таким URL в таблице Products.
    Возвращает True, если URL найден, иначе False.
    """
    query = "SELECT 1 FROM Products WHERE Url = ?"

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (url,))
            return cursor.fetchone() is not None
    except pyodbc.Error as e:
        print(f"Ошибка при проверке URL: {e}")
        return False


def insert_product_files(conn, url, mongo_id, product_id, file_type, creation_time, status):
    query = """
    INSERT INTO ProductsFiles (
        Url, Mongo_Id, ProductId, FileType, CreationTime, Status
    )
    OUTPUT INSERTED.Id
    VALUES (?, ?, ?, ?, ?, ?)
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (url, mongo_id, product_id, file_type, creation_time, status))
        inserted_id = cursor.fetchone()  # Проверяем, есть ли результат

        if inserted_id:  # Если `fetchone()` вернул что-то, берём `Id`
            inserted_id = inserted_id[0]
        else:
            inserted_id = None  # Если нет результата, устанавливаем `None`
        conn.commit()

    return inserted_id
