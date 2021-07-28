import json
import psycopg2
import sys
from typing import Any, Tuple, Union
from pydbml import PyDBML
from jsonschema import validate, ValidationError, SchemaError
from more_itertools import chunked


def validate_json(json_part: Union[Any], schema_file: Union[Any]) -> None:
    """Проверяем входные данные в формате JSON правилам заданным в JSON-схеме."""
    try:
        with open(schema_file, "r") as f:
            schema = json.load(f)
        validate(instance=json_part, schema=schema)
    except SchemaError as error:
        print("Ошибка: ", error)
    except ValidationError as error:
        print("Ошибка: ", error)


def dbml_parser(dbml_file: Any) -> Union[Any]:
    """Получаем структуру БД из DBML-файла."""
    with open(dbml_file) as f:
        source = f.read()
    db_structure = PyDBML(source)
    return db_structure


def create_connection(name: Any, user: Any, password: Any, host: Any, port: Any) -> Any:
    """Устанавливаем соединение с сервером PostgreSQL."""
    connection = psycopg2.connect(
        database=name, user=user, password=password, host=host, port=port
    )
    return connection


def create_tables(connection: Any, db_structure: Any) -> None:
    """Создаём таблицы в БД (PostgreSQL) в соответствии с предоставленной структурой."""
    for i in reversed(range(len(db_structure.tables))):
        cursor = connection.cursor()
        cursor.execute(
            "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = %s)",
            (db_structure.tables[i].name,),
        )
        connection.commit()
        if cursor.fetchone()[0]:
            cursor.close()
            pass
        else:
            create_query = db_structure.tables[i].sql
            cursor.execute(create_query)
            connection.commit()
            cursor.close()


def get_tab_name(db_structure: Any) -> list:
    """Получаем из структуры БД наименования таблиц."""
    tables_name = []
    for table in range(len(db_structure.tables)):
        tables_name.append(db_structure.tables[table].name)
    return tables_name


def get_columns_name(db_structure: Any) -> list:
    """Получаем из структуры БД  наименования столбцов."""
    columns_list = []
    for table in range(len(db_structure.tables)):
        temp_list = []
        for item in range(len(db_structure.tables[table].columns)):
            temp_list.append(db_structure.tables[table].columns[item].name)
        columns_list.append(temp_list)
    return columns_list


def get_good_data(json_as_dict: dict) -> Tuple[tuple[Any, ...], Tuple[list[Any], ...]]:
    """Получаем данные по товарам из JSON-файла."""
    data_for_goods, data_for_shops = [], []
    id_good = None
    for outer_k, v in json_as_dict.items():
        if outer_k == "id":
            data_for_goods.append(json_as_dict[outer_k])
            id_good = json_as_dict[outer_k]
        if outer_k == "name":
            data_for_goods.append(json_as_dict[outer_k])
        if isinstance(v, dict):
            for inner_k in v:
                data_for_goods.append(v[inner_k])
        elif isinstance(v, list):
            for count, _ in enumerate(v):
                for val in v[count].values():
                    data_for_shops.append(val)
    shop_quantity = int(len(data_for_shops) / 2)
    for i in range(0, 3 * shop_quantity, 3):
        data_for_shops.insert(i, id_good)
    return tuple(data_for_goods), tuple(chunked(data_for_shops, 3))


def insert_new_good(connection: Any, tables: list, columns: list, data: tuple) -> None:
    """Добавляем в БД (PostgreSQL) информацию о новом товаре и его наличии в магазинах."""
    goods_table_columns = "(" + ", ".join(columns[1]) + ")"
    goods_records = ", ".join(["%s"] * len(data[0]))
    insert_query_goods = (
        f"INSERT INTO {tables[1]} {goods_table_columns} VALUES ({goods_records})"
    )
    shops_table_columns = "(" + ", ".join(columns[0][1:]) + ")"
    data_for_shops_table = [tuple(data[1][i]) for i in range(len(data[1]))]
    shops_records = ", ".join(["%s"] * len(data_for_shops_table))
    insert_query_shops = (
        f"INSERT INTO {tables[0]} {shops_table_columns} VALUES {shops_records}"
    )
    cursor = connection.cursor()
    cursor.execute(insert_query_goods, data[0])
    cursor.execute(insert_query_shops, data_for_shops_table)
    connection.commit()
    cursor.close()


def check_location(
    connection: Any, tables: list, goods_data: tuple
) -> Union[list, bool]:
    """Находим расхождения между новой и имеющейся в БД (PostgreSQL) информации о магазинах с товаром."""
    cursor = connection.cursor()
    check_query = f"SELECT * from {tables[0]} WHERE id_good = {goods_data[1][0][0]}"
    cursor.execute(check_query)
    result = cursor.fetchall()
    connection.commit()
    cursor.close()
    exist_locations = [i[2] for i in result]
    input_locations = [i[1] for i in goods_data[1]]
    location_diff = list(set(input_locations) - set(exist_locations))
    if len(location_diff) > 0:
        return location_diff
    else:
        return False


def check_good_in_db(connection: Any, tables: list, goods_data: tuple) -> bool:
    """Проверяем наличие информации о товаре в БД (PostgreSQL)."""
    cursor = connection.cursor()
    check_query = f"SELECT * from {tables[1]} WHERE id = {goods_data[0][0]}"
    cursor.execute(check_query)
    query_result = cursor.fetchall()
    connection.commit()
    cursor.close()
    if len(query_result) == 0:
        return False
    else:
        exist_id = query_result[0][0]
        if exist_id == goods_data[0][0]:
            return True
        else:
            return False


def insert_new_location(
    connection: Any,
    location: Union[Any],
    tables: list,
    columns: list,
    goods_data: tuple,
) -> None:
    """Добавляем в БД (PostgreSQL) информацию о новом магазине с товаром."""
    shops_table_columns = "(" + ", ".join(columns[0][1:]) + ")"
    data_for_shops_table = []
    for i in range(len(goods_data[1])):
        for loc in location:
            if loc in goods_data[1][i]:
                data_for_shops_table.append(tuple(goods_data[1][i]))
    shops_records = ", ".join(["%s"] * len(data_for_shops_table))
    location_insert_query = (
        f"INSERT INTO {tables[0]} {shops_table_columns} VALUES {shops_records}"
    )
    cursor = connection.cursor()
    cursor.execute(location_insert_query, data_for_shops_table)
    connection.commit()
    cursor.close()


def update_data_in_db(
    connection: Any, tables: list, columns: list, data: tuple
) -> None:
    """Обновляем информацию об имеющемся в БД (PostgreSQL) товаре."""
    goods_table_columns = columns[1][2:]
    goods_data_table = (data[0][2], data[0][3], data[0][0])
    cursor = connection.cursor()
    data_for_query = ", ".join(
        a + "=%s" for a, _ in zip(goods_table_columns, goods_data_table)
    )
    update_goods_table_query = f"UPDATE {tables[1]} SET {data_for_query} WHERE id=%s"
    cursor.execute(update_goods_table_query, goods_data_table)
    connection.commit()
    all_locations_not_in_db = check_location(connection, tables, data)
    if all_locations_not_in_db:
        insert_new_location(connection, all_locations_not_in_db, tables, columns, data)
    for i in range(len(data[1])):
        shops_data_table = (data[1][i][2], data[1][i][0])
        query = (
            f"UPDATE {tables[0]} SET {columns[0][3]} = %s "
            f"WHERE id_good=%s "
            f"AND to_tsvector({columns[0][2]}) @@ plainto_tsquery('{data[1][i][1]}')"
        )
        cursor.execute(query, shops_data_table)
        connection.commit()
    cursor.close()


def main() -> None:
    """Сохраняем в БД (PostgreSQL) данные о товарах, представленные в формате JSON."""
    connection = None
    dbml_file = "database_doc_goods.dbml"
    json_schema = "goods.schema.json"
    goods_json = "goods.json"
    try:
        connection = create_connection(
            "postgres", "postgres", "mirniy", "localhost", "5432"
        )
        db_struct = dbml_parser(dbml_file)
        tables_name = get_tab_name(db_struct)
        create_tables(connection, db_struct)
        columns_name = get_columns_name(db_struct)
        with open(goods_json, "r") as f:
            document = json.load(f)
        for json_part in document.values():
            validate_json(json_part, json_schema)
            data = get_good_data(json_part)
            if check_good_in_db(connection, tables_name, data):
                update_data_in_db(connection, tables_name, columns_name, data)
            else:
                insert_new_good(connection, tables_name, columns_name, data)
        sys.exit(0)
    except psycopg2.OperationalError as error:
        print(f"Возникла ошибка: '{error}'")
    except OSError:
        print("Необходимые для работы файлы отсутствуют!")
    except SystemExit:
        print("--Программа завершена--")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
