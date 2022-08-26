from database_connection import row_to_df, get_connection, run_sql
import pandas as pd
from datetime import datetime
from conversion_functions import change_postcode_to_first_elem

date_now = datetime.strftime(datetime.now(), "%Y-%m-%d")
sql_var_master = {'entry_database': 'adl_research_pid',
                  'prod_database': 'adl'}

database_creds = "database_creds.conf"

cnxn, crsr = get_connection(config_path=database_creds)
cnxn_adl, crsr_adl = get_connection(config_path=database_creds,
                                    config_section='ADLDB')

sql_path = 'sql/check_latest_date/check_latest_assessment.sql'

crsr, rows = run_sql(sql_loc=sql_path,
                     sql_vars=sql_var_master,
                     cursor=crsr)

df_date = row_to_df(rows=rows, cursor=crsr)
last_date_in_db = datetime.strftime(df_date.CreateDate.dt.date.values[0], "%Y-%m-%d")

tables_in_db = []
for table in crsr.tables(catalog=sql_var_master['entry_database'], schema='dbo'):
    tables_in_db.append(table.table_name)

pid_tables = ['REGISTEREDUSER', 'LifeCurveUsers', 'ADDRESS']

query_all_tables = f"""SELECT 
    *
FROM
    [{sql_var_master['entry_database']}].information_schema.tables"""

crsr, rows = run_sql(sql_loc=sql_path,
                     sql_vars=sql_var_master,
                     cursor=crsr,
                     query=query_all_tables)
df_tables = row_to_df(rows=rows, cursor=crsr)

tables_of_interest = df_tables[(df_tables.TABLE_TYPE == 'BASE TABLE') &
                               ~(df_tables.TABLE_NAME.str.contains('Dome'))]

create_date_in_table = []
date_in_table = []
for table in tables_of_interest.TABLE_NAME.to_list():
    columns_in_table_query = list(crsr_adl.columns(table=table, catalog=sql_var_master['prod_database'], schema='dbo', ))
    columns_in_table = [columns_in_table_query[i][3] for i in range(len(columns_in_table_query))]

    create_date_in_table.append('CreateDate' in columns_in_table)
    if True in [cit.lower().__contains__('date') for cit in columns_in_table]:
        date_in_table.append(True)
    else:
        date_in_table.append(False)

tables_with_createdate = [table for table, cdate_in in
                          zip(tables_of_interest.TABLE_NAME.to_list(), create_date_in_table)
                          if cdate_in]

tables_with_date = [table for table, cdate_in, date_in in
                    zip(tables_of_interest.TABLE_NAME.to_list(), create_date_in_table, date_in_table)
                    if date_in and not cdate_in]

tables_with_no_date = [table for table, cdate_in, date_in in
                       zip(tables_of_interest.TABLE_NAME.to_list(), create_date_in_table, date_in_table)
                       if not date_in and not cdate_in]

sql_variables = sql_var_master.copy()
sql_variables['last_date'] = last_date_in_db
sql_variables['now_date'] = date_now
sql_variables['database'] = sql_var_master['prod_database']

for j, table in enumerate(tables_with_createdate):
    sql_variables['date_col'] = 'CreateDate'
    sql_variables['table_name'] = table

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
    if j > 50:
        break
