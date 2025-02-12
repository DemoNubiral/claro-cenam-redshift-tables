import boto3
import os

import psycopg2
import json
import re
import sys

class Redshift:
    def __init__(self):
        self.client = boto3.client('redshift-data', region_name='us-east-1')

        self.path_file_process = "/redshift-table-process/"
        self.path_file_variables = "/Users/sergiomoreno/programs/nubiral/claro-cenam/claro-cenam-redshift-tables/code-create-tables/environment-variable/"
        self.path_file_variable = "/Users/sergiomoreno/programs/nubiral/claro-cenam/claro-cenam-redshift-tables/code-create-tables/environment-variable/environment.env"
        self.path = os.getcwd()
        self.file = os.path.join(self.path, "log-create-table.txt")


    def execute_query(self, query, database, workgroup):
        try:
            response = self.client.execute_statement(
                Database=database,
                Sql=query,
                WorkgroupName=workgroup
            )
            return response
        except Exception as e:
            return {"statusCode": 500, "error": str(e)}



    def create_table(self, table_creation_query):
        redshift = Redshift()
        response = redshift.execute_query(table_creation_query)
        return response


    def load_table(self, create_table_redshift, connection):        
        connection_cursor =connection.cursor()
        copy_sql = f"""
            {create_table_redshift}
        """
        print("imprime copy sql", copy_sql)
        connection_cursor.execute(copy_sql)
        connection.commit()
        connection_cursor.close()
        connection.close()

    
    def read_file_env(self, path_file):
        self.write_log(self.file, "read_file_env")
        file = open(path_file, 'r')
        env_data = {}
        for line in file:
            if not line.strip() or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            env_data[key] = value
        self.write_log(self.file, f"env_data: {env_data}")
        return env_data


    def parse_env_file(self, file_path, connection, workgroup_name, dbname):
        self.write_log(self.file, "parse_env_file")
        for files in file_path:
            self.write_log(self.file, f"Procesando archivo: {files}")
            data = {}
            current_key = None
            
            with open(files, 'r', encoding='utf-8') as file:
                for line in file:
                    line = line.strip()
                    
                    if line.startswith('#'):
                        current_key = line[1:]  # Remove '#' from the key
                        data[current_key] = ""
                    elif current_key:
                        data[current_key] += line + " "  # Concatenate instead of adding newlines
            
            # Post-process to clean up values
            for key in data:
                data[key] = data[key].replace('"', '').replace("'", '').strip()  # Remove all escaped quotes
            
            # Extract table name
            table_name_match = re.search(r'name_table=(\w+)', data.get("create_table", ""))
            table_name = table_name_match.group(1) if table_name_match else "unknown_table"
            
            # Extract columns as a structured list
            match = re.search(r'columns=\[(.*?)\]', data.get("create_table", ""))
            columns = []
            if match:
                columns_raw = match.group(1)
                for col in columns_raw.split("},"):
                    col = col.replace("{", "").replace("}", "").strip()
                    col_data = dict(item.split(": ") for item in col.split(", "))
                    columns.append(col_data)
            
            # Generate CREATE TABLE statement
            create_table_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            for col in columns:
                print(col.get("type_column"))
                col_name = col.get("name_column")
                col_type = col.get("type_column")
                create_table_stmt += f"    {col_name} {col_type},\n"
            create_table_stmt = create_table_stmt.rstrip(",\n") + "\n)"

            load_table_start = data.get("load_table_start", "").strip()
            load_table_stmt = f"{load_table_start}"


            if create_table_stmt:
                self.write_log(self.file, "Se encontró la sentencia para crear la tabla")
                self.write_log(self.file, f"create_table_stmt: {create_table_stmt}")
                self.create_table(create_table_stmt, workgroup_name, dbname)
            else:
                self.write_log(self.file, "No se encontró la sentencia para crear la tabla")
            
            
            if load_table_stmt:
                self.write_log(self.file, "Se encontró la sentencia para cargar la tabla")
                self.write_log(self.file, f"load_table_stmt: {load_table_stmt}")
                self.load_table(load_table_stmt, connection)
            else:
                self.write_log(self.file, "No se encontró la sentencia para cargar la tabla")   


    def write_log(self, file, text):
        try:
            with open(file, "a", encoding="utf-8") as f:
                f.write(text + "\n")
        except Exception as e:
            print(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    with open('log-create-table.txt', 'w') as fp:
        pass
    try:
        redshift = Redshift()
        file_env = os.listdir(redshift.path_file_variables)
        if file_env:
            environmnet = redshift.read_file_env(redshift.path_file_variable)
            print(environmnet)
            dbname=environmnet['REDSHIFT_DB']
            user=environmnet['REDSHIFT_USER']
            password=environmnet['REDSHIFT_PASSWORD']
            host=environmnet['REDSHIFT_HOST']
            port=environmnet['REDSHIFT_PORT']
            workgroup_name=environmnet['WORKGROUP_NAME']

            connection = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            redshift.write_log(redshift.file, "Conexión establecida con la base de datos")
        else:
            redshift.write_log(redshift.file, "No hay archivos en el directorio")
            sys.exit("No hay archivos en el directorio")
    except Exception as e:
        redshift.write_log(redshift.file, f"Ocurrió un error: {e}")
        print(f"Ocurrió un error: {e}")
        sys.exit("No se pudo establecer conexión con la base de datos")

    files = os.listdir(redshift.path_file_process)
    if files:
        redshift.write_log(redshift.file, f"Se encontraron archivos en el directorio")
        redshift.parse_env_file(files, connection, workgroup_name, dbname)  
    else:
        redshift.write_log(redshift.file, "No hay archivos en el directorio")
        sys.exit("No hay archivos en el directorio")
        print("********************************")
        print("No hay archivos en el directorio")
        print("********************************")

  


