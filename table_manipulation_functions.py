from database_connection import row_to_df, get_connection, run_sql, turn_data_into_insert, drop_table
import pandas as pd
import numpy as np


def get_create_table_schema(table_name,
                            database_name,
                            crsr,
                            archived=False,
                            version=''):
    primary_key_data = list(crsr.primaryKeys(table=table_name,
                                             catalog=database_name,
                                             schema='dbo'))

    table_data = list(crsr.columns(table=table_name,
                                   catalog=database_name,
                                   schema='dbo'))

    all_row_string = []
    has_identity = False
    for column_data in table_data:
        column_name = column_data[3]
        is_nullable = column_data[17]
        null_string = 'NOT NULL' if is_nullable == 'NO' else 'NULL'

        type_name = column_data[5].split(' ')
        if len(type_name) == 1:
            if type_name[0] in ['nvarchar', 'ntext', 'nchar']:
                if column_data[6] == 1073741823:
                    type_string = f"[nvarchar](max)"
                else:
                    type_string = f"[{type_name[0]}]({column_data[6]})"
            elif type_name[0] == 'decimal':
                type_string = f"[{type_name[0]}]({column_data[6]}, {column_data[8]})"

            elif type_name[0] in ['bit', 'datetime', 'int', 'uniqueidentifier']:
                type_string = f"[{type_name[0]}]"
            else:
                print(type_name, column_data)
        else:
            if type_name[1] == 'identity':
                has_identity = True
                type_string = f"[{column_data[5].split(' ')[0]}] IDENTITY(1,1)"

        row_string = f"{column_name} {type_string} {null_string}"
        all_row_string.append(row_string)
    table_definitions = ', \n '.join(all_row_string)

    if archived:
        constraint_string = f"CONSTRAINT [{primary_key_data[0][5]}_archived] PRIMARY KEY CLUSTERED ([{primary_key_data[0][3]}] ASC)"
        out_table_name = f"{table_name}_archived"
    else:
        version_tag = f"_{version}" if version != '' else ""
        constraint_string = f"CONSTRAINT [{primary_key_data[0][5]}{version_tag}] PRIMARY KEY CLUSTERED ([{primary_key_data[0][3]}] ASC)"
        out_table_name = f"{table_name}_{version}" if version != '' else table_name

    sql_input = {'table_name': out_table_name,
                 'table_schema': 'dbo',
                 'table_definitions': table_definitions,
                 'table_constraints': constraint_string,
                 'database': database_name,
                 'table_name_old': table_name,
                 'has_identity': has_identity}
    return sql_input


def alter_table_for_different_columns_prod_entry(table_name,
                                                 prod_database,
                                                 entry_database,
                                                 crsr_prod,
                                                 crsr_entry):
    create_table_schema_prod = get_create_table_schema(table_name=table_name,
                                                       database_name=prod_database,
                                                       crsr=crsr_prod)
    create_table_schema_entry = get_create_table_schema(table_name=table_name,
                                                        database_name=entry_database,
                                                        crsr=crsr_entry)

    master_table_def = create_table_schema_prod['table_definitions']
    current_table_def = create_table_schema_entry['table_definitions']

    split_master_table_def = master_table_def.split(', \n ')
    split_current_table_def = current_table_def.split(', \n ')

    missing_columns = [smtd for smtd in split_master_table_def if smtd not in split_current_table_def]

    alter_table_columns = []
    for missing_col in missing_columns:
        alter_table_columns.append({'table_name': create_table_schema_prod['table_name'],
                                    'column_schema': missing_col,
                                    'database': create_table_schema_entry['database']
                                    })
    return alter_table_columns


def find_foreign_keys_in_table(table_list: list,
                               entry_database: str,
                               entry_cursor):
    table_requires = []
    for table_name in table_list:
        foreign_key_data = list(entry_cursor.foreignKeys(table=table_name,
                                                         catalog=entry_database,
                                                         schema='dbo'))

        for table_with_keys in foreign_key_data:
            if len(table_with_keys) > 0:
                table_requires.append([table_with_keys[6], table_with_keys[2]])
    return pd.DataFrame(data=table_requires, columns=['primary_key', 'foreign_key'])


def sql_string_fix(in_string):
    return in_string.replace("'", "''")


def turn_large_data_into_insert(dataframe: pd.DataFrame, table_name: str, database_name: str, columns: list = None):
    """
    dataframe columns MUST match the columns in the table if columns=None,
    otherwise specify the columns in the order they are in the df
    """
    data_string = ''
    oversized_data = []
    for row in dataframe.itertuples():
        row_string = '(' + ", ".join(['NULL' if type(elem) in [type(None), type(pd.NaT)]
                                      else str(elem) if type(elem) in [int, float, bool]
                                      else f"'{str(pd.to_datetime(elem))[:23]}'" if type(elem) in [np.datetime64,
                                                                                                   pd.Timestamp]
                                      else f"'{elem}'" if type(elem) != str
                                      else f"'{sql_string_fix(elem)}'"
                                      for elem in row[1:]]) + ')'
        if data_string != '':
            data_string = data_string + ', ' + row_string
        else:
            data_string = data_string + row_string
        if len(data_string) > 9000:
            oversized_data.append(data_string)
            data_string = ''

    if not columns:
        columns = ', '.join([str(col) for col in dataframe.columns])

    if len(oversized_data) > 0:
        insert_queries = []
        for data_string in oversized_data:
            insert_query = f"""
                            INSERT INTO [{database_name}].[dbo].[{table_name}] ({columns})
                            VALUES
                                    {data_string}"""
            insert_queries.append(insert_query)
        return insert_queries
    else:
        insert_query = f"""
                        INSERT INTO [{database_name}].[dbo].[{table_name}] ({columns})
                        VALUES
                                {data_string}"""
        return insert_query
