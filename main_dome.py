from database_connection import row_to_df, get_connection, run_sql, turn_data_into_insert
import pandas as pd
from datetime import datetime
from conversion_functions import change_postcode_to_first_elem

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


sql_var_master['database'] = sql_var_master['entry_database']
crsr, rows = run_sql(sql_loc=sql_path,
                     sql_vars=sql_var_master,
                     cursor=crsr)

df_date = row_to_df(rows=rows, cursor=crsr)
last_date_in_db = datetime.strftime(df_date.CreateDate.dt.date.values[0], "%Y-%m-%d")

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
                               (df_tables.TABLE_SCHEMA == 'dbo')]

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

    data_to_insert = turn_data_into_insert(dataframe=df_converted,
                                           database_name=sql_variables['entry_database'],
                                           table_name=table)
    data_entry_list.append(data_to_insert)
    # run_sql(cursor=crsr,
    #         query=data_to_insert,
    #         fetch_results=False)
    # cnxn.commit()
