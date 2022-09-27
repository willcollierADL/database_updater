from database_connection import row_to_df, get_connection, run_sql, turn_large_data_into_insert, delete_records
import pandas as pd
from datetime import datetime, timedelta
from conversion_functions import change_postcode_to_first_elem
from table_manipulation_functions import sql_string_fix, find_foreign_keys_in_table


def convert_to_passable(data_list):
    return ['NULL' if type(elem) in [type(None), type(pd.NaT)]
            else str(elem) if type(elem) in [int, float, bool]
    else f"'{elem}'" if type(elem) != str
    else f"'{sql_string_fix(elem)}'"
            for elem in data_list]


def delete_added_records_for_repeat_entry(connex, cursr, entry_database, tablename, new_records):
    primary_keys = list(cursr.primaryKeys(table=tablename,
                                          catalog=entry_database,
                                          schema='dbo'))
    for new_record_elem in new_records:
        record_cols = new_record_elem.split('VALUES')[0].split('(')[-1].split(')')[0].split(',')
        record_val_idx = record_cols.index(primary_keys[0][3])

        record_string = [nr.split('(')[-1].split(', ')[record_val_idx]
                         for nr in new_record_elem.split('VALUES\n')[-1].split('), (')]

        delete_records(connection=connex,
                       cursor=cursr,
                       database_name=entry_database,
                       table_name=tablename,
                       record_column=primary_keys[0][3],
                       records_remove_string=', '.join(convert_to_passable([int(rs) for rs in record_string
                                                                            if '"' not in rs and rs != 'NULL']))
                       )


date_now = datetime.strftime(datetime.now() + timedelta(days=1), "%Y-%m-%d")
sql_var_master = {'entry_database': 'adl_research_pid',
                  'prod_database': 'adl'}

database_creds = "database_creds.conf"

cnxn, crsr = get_connection(config_path=database_creds)
cnxn_adl, crsr_adl = get_connection(config_path=database_creds,
                                    config_section='ADLDB')
cnxn_pid_rem, crsr_pid_rem = get_connection(config_path=database_creds,
                                            config_section='ADLPIDREM',
                                            use_database=True)

sql_var_master['database'] = sql_var_master['entry_database']


def find_last_date_in_database_table(sql_path_for_table, sql_variable, cursor):
    cursor, rows = run_sql(sql_loc=sql_path_for_table,
                           sql_vars=sql_variable,
                           cursor=cursor)
    if rows:
        if rows[0][0]:
            df_date = row_to_df(rows=rows, cursor=cursor)
            return datetime.strftime(pd.to_datetime(df_date[sql_variable['date_col']].values[0]),
                                     "%Y-%m-%d %H:%M:%S.%f")[:23], cursor
        else:
            return 'NULL', cursor
    else:
        return datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S.%f")[:23], cursor


tables_in_db = []
for table in crsr_adl.tables(catalog=sql_var_master['prod_database'], schema='dbo'):
    tables_in_db.append(table.table_name)

pid_tables = ['REGISTEREDUSER', 'LifeCurveUsers', 'ADDRESS']
special_case_tables = ['ASSESSMENTSAVEDMATCHCRITERIA']

query_all_tables = f"""SELECT 
    *
FROM
    [{sql_var_master['prod_database']}].information_schema.tables"""

crsr_adl, rows = run_sql(cursor=crsr_adl,
                         query=query_all_tables)
df_tables = row_to_df(rows=rows, cursor=crsr_adl)

tables_of_interest = df_tables[(df_tables.TABLE_TYPE == 'BASE TABLE') &
                               ~(df_tables.TABLE_NAME.str.contains('Dome')) &
                               ~(df_tables.TABLE_NAME.str.contains('default')) &
                               ~(df_tables.TABLE_NAME.str.contains('ADLSTEP')) &
                               (df_tables.TABLE_NAME != 'sysdiagrams') &
                               (df_tables.TABLE_NAME != 'ASSESSMENTSAVEDMATCHCRITERIA') &
                               (df_tables.TABLE_NAME != 'AspNetUsers')]

create_date_in_table = []
tables_with_createdate, tables_with_date, tables_with_no_date = [], [], []
date_in_table = []
for table in tables_of_interest.TABLE_NAME.to_list():
    columns_in_table_query = list(crsr_adl.columns(table=table, catalog=sql_var_master['prod_database'], schema='dbo'))
    columns_in_table = [columns_in_table_query[i][3] for i in range(len(columns_in_table_query))]

    if 'CreateDate' in columns_in_table:
        tables_with_createdate.append([table, 'CreateDate'])
    elif 'CreatedAt' in columns_in_table:
        tables_with_createdate.append([table, 'CreatedAt'])
    elif True in [cit.lower().__contains__('date') for cit in columns_in_table]:
        cols_w_date = [cit for cit in columns_in_table if cit.lower().__contains__('date')]
        if len(cols_w_date) == 1:
            tables_with_date.append([table, cols_w_date[0]])
            continue
        create_col = [cwd for cwd in cols_w_date if 'create' in cwd.lower()]
        if len(create_col) > 0:
            print(create_col)
            tables_with_date.append([table, create_col[0]])
            continue
        print(cols_w_date)
    else:
        tables_with_no_date.append([table, ''])

sql_variables = sql_var_master.copy()
sql_variables['now_date'] = date_now
sql_variables['database'] = sql_var_master['prod_database']

data_entry_list = []
data_entry_dict = {}
all_date_tables = tables_with_createdate + tables_with_date
for j, (table, date_column) in enumerate(all_date_tables):
    print(f"{j} from {len(all_date_tables)}")
    sql_variables['date_col'] = date_column
    sql_variables['table_name'] = table

    sql_path_check_latest = 'sql/check_latest_date/check_latest_assessment.sql'

    sql_variables['last_date'], crsr_pid_rem = find_last_date_in_database_table(
        sql_path_for_table=sql_path_check_latest,
        sql_variable=sql_variables,
        cursor=crsr_pid_rem)

    if table not in pid_tables:
        sql_path = 'sql/collect_data/generic_check_columns.sql'
        crsr_adl, rows = run_sql(sql_loc=sql_path,
                                 sql_vars=sql_variables,
                                 cursor=crsr_adl)
        if rows[0][0] == 1:
            sql_path = 'sql/collect_data/generic_collect_data.sql'
            crsr_adl, rows = run_sql(sql_loc=sql_path,
                                     sql_vars=sql_variables,
                                     cursor=crsr_adl)
            df_converted = row_to_df(rows=rows, cursor=crsr_adl)
        else:
            df_converted = pd.DataFrame()

    else:
        sql_path = f'sql/collect_data/collect_pid_data_from_{table.lower()}.sql'

        crsr_adl, rows = run_sql(sql_loc=sql_path,
                                 sql_vars=sql_variables,
                                 cursor=crsr_adl)
        df_table_content = row_to_df(rows=rows, cursor=crsr_adl)

        if 'Postcode' in df_table_content.columns:
            df_converted = change_postcode_to_first_elem(df=df_table_content)
        else:
            df_converted = df_table_content.copy()
        del df_table_content

    data_to_insert = turn_large_data_into_insert(dataframe=df_converted,
                                                 database_name=sql_variables['entry_database'],
                                                 table_name=table)
    if type(data_to_insert) != list:
        data_to_insert = [data_to_insert]
    data_entry_dict[table] = data_to_insert


all_table_names = [adt[0] for adt in all_date_tables] + [twnd[0] for twnd in tables_with_no_date]
table_order_df = find_foreign_keys_in_table(table_list=all_table_names,
                                            entry_database=sql_var_master['entry_database'],
                                            entry_cursor=crsr_pid_rem)

keep_shuffling = True
ordered_table_option = all_table_names.copy()
while keep_shuffling:
    sv_ordered_table_option = ordered_table_option.copy()
    for i, table_elem in enumerate(all_table_names):
        if table_elem in table_order_df.foreign_key.to_list():
            primary_tables = table_order_df[table_order_df.foreign_key == table_elem]
            te_index = ordered_table_option.index(table_elem)
            pt_index = [[pt, ordered_table_option.index(pt)] for pt in primary_tables.primary_key.unique().tolist()
                        if
                        pt in ordered_table_option]
            tables_to_move = [pti[0] for pti in pt_index if pti[1] < te_index]
            [ordered_table_option.remove(ttm) for ttm in tables_to_move]
            ordered_table_option = ordered_table_option + tables_to_move

    if sv_ordered_table_option == ordered_table_option:
        keep_shuffling = False

for j, table in enumerate(ordered_table_option):
    print(f"{j} from {len(ordered_table_option)}")

    if table in [adt[0] for adt in all_date_tables]:

        data_to_insert = data_entry_dict[table]

        crsr_pid_rem, rows = run_sql(cursor=crsr_pid_rem,
                                     query=f"""select count(1) where exists (SELECT *
                            FROM sys.identity_columns
                            WHERE OBJECT_NAME(object_id) = '{table}')""")
        if rows[0][0] == 1:
            run_sql(cursor=crsr_pid_rem,
                    query=f"SET IDENTITY_INSERT [{sql_var_master['entry_database']}].[dbo].[{table}] ON",
                    fetch_results=False)

        for dti in data_to_insert:
            if dti.split('VALUES\n')[-1].replace(' ', ''):
                run_sql(cursor=crsr_pid_rem,
                        query=dti.replace(', True', ', 1').replace(', False', ', 0').replace(', nan', ', NULL'),
                        fetch_results=False)
                cnxn_pid_rem.commit()
        if rows[0][0] == 1:
            run_sql(cursor=crsr_pid_rem,
                    query=f"SET IDENTITY_INSERT [{sql_var_master['entry_database']}].[dbo].[{table}] OFF",
                    fetch_results=False)
        print(f'[{table}]: data inserted into table')
    else:

        skip_insert = False
        sql_variables['table_name'] = table
        sql_variables['database'] = sql_variables['prod_database']

        sql_path = 'sql/collect_data/generic_check_columns.sql'
        crsr_adl, rows = run_sql(sql_loc=sql_path,
                                 sql_vars=sql_variables,
                                 cursor=crsr_adl)
        if rows[0][0] == 1:
            sql_path = 'sql/collect_data/generic_collect_data_no_date.sql'

            crsr_adl, rows = run_sql(sql_loc=sql_path,
                                     sql_vars=sql_variables,
                                     cursor=crsr_adl)
            df_converted = row_to_df(rows=rows, cursor=crsr_adl)

            sql_variables['database'] = sql_variables['entry_database']
            crsr, rows = run_sql(sql_loc=sql_path,
                                 sql_vars=sql_variables,
                                 cursor=crsr)
            df_existing = row_to_df(rows=rows, cursor=crsr_adl)

            merged = df_converted.merge(df_existing, indicator=True, how='outer')
            new_data = merged[(merged['_merge'] == 'left_only')]

            primary_keys = list(crsr_pid_rem.primaryKeys(table=table,
                                                         catalog=sql_variables['entry_database'],
                                                         schema='dbo'))
            if len(primary_keys) > 0:
                primary_key = primary_keys[0][3]
                primary_vals_in_existing = [nd_pk for nd_pk in new_data[primary_key].to_list() if
                                            nd_pk in df_existing[primary_key].to_list()]
            else:
                primary_vals_in_existing = []

            if new_data.shape[0] == 0:
                skip_insert = True
        else:
            skip_insert = True
        if not skip_insert:
            if len(primary_vals_in_existing) > 0:
                print('deleting data')
                delete_records(connection=cnxn_pid_rem,
                               cursor=crsr_pid_rem,
                               database_name=sql_variables['entry_database'],
                               table_name=table,
                               record_column=primary_key,
                               records_remove_string=', '.join(convert_to_passable(primary_vals_in_existing)))

            data_to_insert = turn_large_data_into_insert(dataframe=new_data.drop(columns='_merge'),
                                                         database_name=sql_variables['entry_database'],
                                                         table_name=table)
            if type(data_to_insert) != list:
                data_to_insert = [data_to_insert]

            for dti in data_to_insert:
                if dti.split('VALUES\n')[-1].replace(' ', ''):
                    run_sql(cursor=crsr,
                            query=dti.replace(', True', ', 1').replace(', False', ', 0'),
                            fetch_results=False)
                    cnxn.commit()
