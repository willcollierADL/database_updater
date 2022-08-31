SET IDENTITY_INSERT [{database}].[dbo].[{archive_table}] ON
INSERT INTO [{database}].[{table_schema}].[{archive_table}] ({table_columns})
SELECT {table_columns} FROM [{database}].[dbo].[{table_name}]
SET IDENTITY_INSERT [{database}].[dbo].[{archive_table}] OFF
