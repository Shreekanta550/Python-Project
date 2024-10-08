import pypyodbc as odbc
import pandas as pd
from tkinter import Tk, filedialog
import os

def clean_string(input_str):
    cleaned_str = input_str.lower().replace(" ", "_").replace("?", "") \
        .replace("-", "_").replace("/", "_").replace("\\", "_").replace("$", "") \
        .replace("%", "").replace(")", "").replace("(", "")
    return cleaned_str

DRIVER = 'SQL Server'
SERVER_NAME = 'Shree\SQLEXPRESS'
DATABASE_NAME = 'shree'

def connection_string(drivers, server_name, database_name):
    conn_string = f"DRIVER={{{drivers}}};SERVER={server_name};DATABASE={database_name};Trust_Connection=yes;"
    return conn_string

try:
    Tk().withdraw()  
    csv_file_path = filedialog.askopenfilename(title="Select CSV file", filetypes=[("CSV Files", "*.csv")])

    table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    clean_tbl_name = clean_string(table_name)

    df = pd.read_csv(csv_file_path)
    df.columns = [clean_string(col) for col in df.columns]

    conn = odbc.connect(connection_string(DRIVER, SERVER_NAME, DATABASE_NAME))
    cursor = conn.cursor()

    create_table_sql = f"CREATE TABLE {clean_tbl_name} ({', '.join([f'{col} NVARCHAR(MAX)' for col in df.columns])})"
    cursor.execute(create_table_sql)
    
    for index, row in df.dropna().iterrows():
        insert_values = tuple(row)
        cursor.execute(f"INSERT INTO {clean_tbl_name} VALUES " + str(insert_values))
    
    conn.commit()
    
    print(f'Data inserted successfully into table {clean_tbl_name}.')
except odbc.DatabaseError as e:
    print('Database Error:')
    print(str(e.value[1]))
except odbc.Error as e:
    print('Connection Error:')
    print(str(e.value[1]))
except Exception as e:
    print('An error occurred:')
    print(str(e))
finally:
    cursor.close()
    conn.close()
