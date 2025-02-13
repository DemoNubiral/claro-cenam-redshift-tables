import boto3
import os
import datetime
import psycopg2
import json
import re
import sys

class Redshift:
    def __init__(self):
        self.client = boto3.client('redshift-data', region_name='us-east-1')
        self.client_s3 = boto3.client('s3')
        self.path_file_process = "redshift-table-process/"
        self.path = os.getcwd()
        self.file = "create-table-redshift-log.txt"


    def execute_query(self, query, database, workgroup, bucket_name):
        try:
            self.write_log(self.file, "execute_query", bucket_name)
            response = self.client.execute_statement(
                Database=database,
                Sql=query,
                WorkgroupName=workgroup
            )
            self.write_log(self.file, f"response: {response}, se ejecuta la query de creacion de tabla.", bucket_name)
            return response
        except Exception as e:
            self.write_log(self.file, f"Ocurrió un error: {e}", bucket_name)
            return {"statusCode": 500, "error": str(e)}



    def create_table(self, table_creation_query):
        try:
            self.write_log(self.file, "create_table", bucket_name)
            redshift = Redshift()
            response = redshift.execute_query(table_creation_query, dbname, workgroup_name, bucket_name)
            if response['statusCode'] != 500:
                self.write_log(self.file, f"response: {response}, se crea la tabla satisfactoriamente", bucket_name)
                return response
            else: 
                self.write_log(self.file, f"response: {response}, falla en la creacion de la tabla", bucket_name)
                return {"statusCode": 500, "error": str(response)}
        except Exception as e:
            self.write_log(self.file, f"Ocurrió un error: {e}", bucket_name)
            return {"statusCode": 500, "error": str(e)}



    def load_table(self, create_table_redshift, connection, bucket_name):
        try:
            self.write_log(self.file, "load_table", bucket_name)        
            connection_cursor =connection.cursor()
            copy_sql = f"""
                {create_table_redshift}
            """
            self.write_log(self.file, f"copy_sql: {copy_sql}", bucket_name)
            connection_cursor.execute(copy_sql)
            connection.commit()
            connection_cursor.close()
            connection.close()
            self.write_log(self.file, "Carga de tabla exitosa", bucket_name)
        except Exception as e:
            self.write_log(self.file, f"Ocurrió un error: {e}", bucket_name)
            print(f"Ocurrió un error: {e}")


    def parse_env_file(self, file_path, connection, workgroup_name, dbname, bucket_name):
        self.write_log(self.file, "parse_env_file", bucket_name)
        for files in file_path:
            self.write_log(self.file, f"Procesando archivo: {files}")
            data = {}
            current_key = None
            
            with open("redshift-table-process/"+files, 'r', encoding='utf-8') as file:
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
                col_name = col.get("name_column")
                col_type = col.get("type_column")
                create_table_stmt += f"    {col_name} {col_type},\n"
            create_table_stmt = create_table_stmt.rstrip(",\n") + "\n)"

            load_table_start = data.get("load_table_start", "").strip()
            load_table_stmt = f"{load_table_start}"


            if create_table_stmt:
                self.write_log(self.file, "Se encontró la sentencia para crear la tabla", bucket_name)
                self.write_log(self.file, f"create_table_stmt: {create_table_stmt}", bucket_name)
                response = self.create_table(create_table_stmt, workgroup_name, dbname, bucket_name)
                if response['statusCode'] == 500:
                    sys.exit("No se pudo crear la tabla")
            else:
                self.write_log(self.file, "No se encontró la sentencia para crear la tabla", bucket_name)
            
            
            if load_table_stmt:
                self.write_log(self.file, "Se encontró la sentencia para cargar la tabla", bucket_name)
                self.write_log(self.file, f"load_table_stmt: {load_table_stmt}", bucket_name)
                self.load_table(load_table_stmt, connection, bucket_name)
            else:
                self.write_log(self.file, "No se encontró la sentencia para cargar la tabla", bucket_name)   


    def write_log(self, file, text, bucket_name):
        try:
            try:
                response = self.client_s3.get_object(Bucket=bucket_name, Key=file)
                existing_content = response['Body'].read().decode('utf-8')
            except self.client_s3.exceptions.NoSuchKey:
                existing_content = ""
            
            # Agregar el nuevo log con la fecha y hora
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_content = f"{existing_content} \n {timestamp} - {text}\n"
            
            # Subir el archivo actualizado a self.client_s3
            self.client_s3.put_object(Bucket=bucket_name, Key=file, Body=new_content)
            print("Log escrito en S3 correctamente.")
        except Exception as e:
            print(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    try:
        print("********************************")
        print("se inicia proceso de creación de tablas en redshift")
        print("********************************")
        redshift = Redshift()
        bucket_name=os.getenv('BUCKET_NAME')
        dbname=os.getenv('REDSHIFT_DB')
        user=os.getenv('REDSHIFT_USER')
        password=os.getenv('REDSHIFT_PASSWORD')
        host=os.getenv('REDSHIFT_HOST')
        port=os.getenv('REDSHIFT_PORT')
        workgroup_name=os.getenv('WORKGROUP_NAME')

        redshift.write_log(redshift.file, "Inicio del proceso de creación de tablas en redshift", bucket_name)
        
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        redshift.write_log(redshift.file, "Conexión establecida con la base de datos", bucket_name)

    except Exception as e:
        redshift.write_log(redshift.file, f"Ocurrió un error: {e}", bucket_name)
        print(f"Ocurrió un error: {e}")
        sys.exit("No se pudo establecer conexión con la base de datos")

    files = os.listdir(redshift.path_file_process)
    if files:
        redshift.write_log(redshift.file, f"Se encontraron archivos en el directorio", bucket_name)
        redshift.parse_env_file(files, connection, workgroup_name, dbname, bucket_name)  
    else:
        redshift.write_log(redshift.file, "No hay archivos en el directorio", bucket_name)
        sys.exit("No hay archivos en el directorio")
        print("********************************")
        print("No hay archivos en el directorio")
        print("********************************")

  


