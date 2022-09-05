from database_connection import row_to_df, get_connection, run_sql, turn_large_data_into_insert, drop_table, \
    delete_records
import pandas as pd
import numpy as np
from datetime import datetime
from conversion_functions import change_postcode_to_first_elem
from table_manipulation_functions import get_create_table_schema, alter_table_for_different_columns_prod_entry, \
    find_foreign_keys_in_table


date_now = datetime.strftime(datetime.now(), "%Y-%m-%d")
sql_var_master = {'entry_database': 'adl_research_pid',
                  'prod_database': 'adldmev0300_beta'}

database_creds = "database_creds.conf"

cnxn, crsr = get_connection(config_path=database_creds)
cnxn_adl, crsr_adl = get_connection(config_path=database_creds,
                                    config_section='ADLDB')

sql_path = 'sql/check_latest_date/check_latest_dome.sql'

query_all_tables = f"""SELECT 
    *
FROM
    [{sql_var_master['prod_database']}].information_schema.tables"""

crsr, rows = run_sql(cursor=crsr,
                     query=query_all_tables)
df_tables = row_to_df(rows=rows, cursor=crsr)

# sql_var_master['database'] = sql_var_master['entry_database']
# crsr, rows = run_sql(sql_loc=sql_path,
#                      sql_vars=sql_var_master,
#                      cursor=crsr)
#
# df_date = row_to_df(rows=rows, cursor=crsr)
# last_date_in_db = datetime.strftime(df_date.CreateDate.dt.date.values[0], "%Y-%m-%d")

last_date_in_db = "2016-01-01"

tables_in_db = []
for table in crsr.tables(catalog=sql_var_master['prod_database'], schema='dbo'):
    tables_in_db.append(table.table_name)

pid_tables = ['REGISTEREDUSER', 'LifeCurveUsers', 'ADDRESS']

# query_all_tables = f"""SELECT
#     *
# FROM
#     [{sql_var_master['prod_database']}].information_schema.tables"""
#
# crsr_adl, rows = run_sql(cursor=crsr_adl,
#                          query=query_all_tables)
# df_tables = row_to_df(rows=rows, cursor=crsr_adl)

tables_of_interest = df_tables[(df_tables.TABLE_TYPE == 'BASE TABLE') &
                               (df_tables.TABLE_SCHEMA == 'dbo') &
                               (df_tables.TABLE_NAME != 'sysdiagrams')]

create_date_in_table = []
tables_with_createdate, tables_with_date, tables_with_no_date = [], [], []
date_in_table = []
for table in tables_of_interest.TABLE_NAME.to_list():
    columns_in_table_query = list(crsr.columns(table=table, catalog=sql_var_master['prod_database'], schema='dbo'))
    columns_in_table = [columns_in_table_query[i][3] for i in range(len(columns_in_table_query))]

    if 'CreateDate' in columns_in_table:
        tables_with_createdate.append([table, 'CreateDate'])
    elif 'CreatedAt' in columns_in_table:
        tables_with_createdate.append([table, 'CreatedAt'])
        # create_date_in_table.append('CreateDate' in columns_in_table or 'CreatedAt' in columns_in_table)
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
sql_variables['last_date'] = last_date_in_db
sql_variables['now_date'] = date_now
sql_variables['database'] = sql_var_master['prod_database']

sql_path = 'sql/collect_data/generic_check_columns.sql'
tables_with_no_date_and_data = []
for table, column in tables_with_no_date:
    sql_variables['table_name'] = table
    crsr, rows = run_sql(sql_loc=sql_path,
                         sql_vars=sql_variables,
                         cursor=crsr)
    if rows[0][0] == 1:
        tables_with_no_date_and_data.append([table, column])

tables_with_date_and_data = []
for table, column in tables_with_date:
    sql_variables['table_name'] = table
    crsr, rows = run_sql(sql_loc=sql_path,
                         sql_vars=sql_variables,
                         cursor=crsr)
    if rows[0][0] == 1:
        tables_with_date_and_data.append([table, column])

tables_with_createdate_and_data = []
for table, column in tables_with_createdate:
    sql_variables['table_name'] = table
    crsr, rows = run_sql(sql_loc=sql_path,
                         sql_vars=sql_variables,
                         cursor=crsr)
    if rows[0][0] == 1:
        tables_with_createdate_and_data.append([table, column])

data_entry_list = []
data_entry_dict = {}
for j, (table, date_column) in enumerate(tables_with_createdate):
    print(f"{j} from {len(tables_with_createdate)}")
    sql_variables['date_col'] = 'CreateDate'
    sql_variables['table_name'] = table

    if table not in pid_tables:
        sql_path = 'sql/collect_data/generic_check_columns.sql'
        crsr, rows = run_sql(sql_loc=sql_path,
                             sql_vars=sql_variables,
                             cursor=crsr)
        if rows[0][0] == 1:
            sql_path = 'sql/collect_data/generic_collect_data.sql'
            crsr, rows = run_sql(sql_loc=sql_path,
                                 sql_vars=sql_variables,
                                 cursor=crsr)
            df_converted = row_to_df(rows=rows, cursor=crsr)
        else:
            df_converted = pd.DataFrame()
    else:
        sql_path = f'sql/collect_data/collect_pid_data_from_{table.lower()}.sql'

        crsr, rows = run_sql(sql_loc=sql_path,
                             sql_vars=sql_variables,
                             cursor=crsr)
        df_table_content = row_to_df(rows=rows, cursor=crsr)

        if 'Postcode' in df_table_content.columns:
            df_converted = change_postcode_to_first_elem(df=df_table_content)
        else:
            df_converted = df_table_content.copy()
        del df_table_content
    # if j > 50 or df_converted.shape[0] > 0:
    #     break
    # if df_converted.shape[0] > 0:
    #     break
    data_to_insert = turn_large_data_into_insert(dataframe=df_converted,
                                                 database_name=sql_variables['entry_database'],
                                                 table_name=table)
    data_entry_list.append(data_to_insert)
    if type(data_to_insert) != list:
        data_to_insert = [data_to_insert]
    data_entry_dict[table] = data_to_insert

"""
Run an insert!
"""

cnxn, crsr = get_connection(config_path=database_creds)
cnxn_dme, crsr_dme = get_connection(config_path=database_creds,
                                    config_section='DMEV1BETA',
                                    use_database=True)
cnxn_pid_rem, crsr_pid_rem = get_connection(config_path=database_creds,
                                            config_section='ADLPIDREM',
                                            use_database=True)

alter_table_columns = alter_table_for_different_columns_prod_entry(table_name='DomeCriteria',
                                                                   prod_database=sql_var_master['prod_database'],
                                                                   entry_database=sql_var_master['entry_database'],
                                                                   crsr_prod=crsr_dme,
                                                                   crsr_entry=crsr_pid_rem)

table_order_df = find_foreign_keys_in_table(table_list=tables_of_interest.TABLE_NAME.to_list(),
                                            entry_database=sql_var_master['entry_database'],
                                            entry_cursor=crsr_pid_rem)
# pd.read_csv("data/table_order/table_delete_order.csv")

all_tables = np.unique(
    table_order_df["primary_key"].unique().tolist() + table_order_df["foreign_key"].unique().tolist())

table_order_after, table_order_before, table_order_master, table_order_middle = [], [], [], []
while False in [at in table_order_master for at in all_tables]:
    for table_name in all_tables:
        if table_name not in table_order_master:
            if table_name not in table_order_df.foreign_key.to_list():
                table_order_master.append(table_name)
                table_order_after.append(table_name)
            else:
                if table_name not in table_order_df.primary_key.to_list():
                    table_order_master.append(table_name)
                    table_order_before.append(table_name)
                else:
                    table_order_master.append(table_name)
                    table_order_middle.append(table_name)

table_order_keyed = table_order_after + table_order_middle + table_order_before

table_order_overall = list(set(data_entry_dict.keys()).difference(table_order_keyed)) + table_order_keyed

all_table_options = list(data_entry_dict.keys())

keep_shuffling = True
ordered_table_option = all_table_options.copy()
while keep_shuffling:
    sv_ordered_table_option = ordered_table_option.copy()
    for i, table_elem in enumerate(all_table_options):
        if table_elem in table_order_df.foreign_key.to_list():
            primary_tables = table_order_df[table_order_df.foreign_key == table_elem]
            te_index = ordered_table_option.index(table_elem)
            pt_index = [[pt, ordered_table_option.index(pt)] for pt in primary_tables.primary_key.unique().tolist()]
            tables_to_move = [pti[0] for pti in pt_index if pti[1] > te_index]
            [ordered_table_option.remove(ttm) for ttm in tables_to_move]
            ordered_table_option = tables_to_move + ordered_table_option

    if sv_ordered_table_option == ordered_table_option:
        keep_shuffling = False

for j, table_oo in enumerate(ordered_table_option):
    print(j, len(ordered_table_option))
    create_table_schema_master = get_create_table_schema(table_name=table_oo,
                                                         database_name=sql_var_master['prod_database'],
                                                         crsr=crsr_dme)
    create_table_schema_master['database'] = sql_var_master['entry_database']
    print(f'[{table_oo}]: created table schema master')
    drop_table(connection=cnxn,
               cursor=crsr,
               database_name=sql_var_master['entry_database'],
               table_name=table_oo
               )
    print(f'[{table_oo}]: dropped table')

    run_sql(sql_loc='sql/create_table_script/generic_create_table.sql',
            cursor=crsr_pid_rem,
            sql_vars=create_table_schema_master,
            fetch_results=False)
    cnxn_pid_rem.commit()
    print(f'[{table_oo}]: rebuilt table')

    run_sql(cursor=crsr_pid_rem,
            query=f"SET IDENTITY_INSERT [{sql_var_master['entry_database']}].[dbo].[{table_oo}] ON",
            fetch_results=False)
    if table_oo in list(data_entry_dict.keys()):
        for insert_query in data_entry_dict[table_oo]:
            if insert_query.split('VALUES\n')[1].replace(' ', ''):

                run_sql(cursor=crsr_pid_rem,
                        query=insert_query.replace(', True', ', 1').replace(', False', ', 0'),
                        fetch_results=False)

                crsr_pid_rem.commit()
                print("entered data")
            else:
                print(f'no data in table {table_oo}')
        run_sql(cursor=crsr_pid_rem,
                query=f"SET IDENTITY_INSERT [{sql_var_master['entry_database']}].[dbo].[{table_oo}] OFF",
                fetch_results=False)
        print(f'[{table_oo}]: data inserted into table')
    else:
        print(f'[{table_oo}]: not in {sql_var_master["prod_database"]}')
