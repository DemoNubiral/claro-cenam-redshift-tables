import boto3
import os
import logging
import psycopg2


class Redshift:
    def __init__(self):
        self.client = boto3.client('redshift-data', region_name='us-east-1')
        self.connection = psycopg2.connect( 
            dbname=os.environ['REDSHIFT_DB'],
            user=os.environ['REDSHIFT_USER'],
            password=os.environ['REDSHIFT_PASSWORD'],
            host=os.environ['REDSHIFT_HOST'],
            port=os.environ['REDSHIFT_PORT']
        )

    def execute_query(self, query):
        try:
            response = self.client.execute_statement(
                Database="kopiclouddb",
                Sql=query,
                # DbUser="kopiadmin",
                WorkgroupName="kopicloud-workgroup"
            )
            logging.info(f"Respuesta de Redshift: {response}")
            return response
        except Exception as e:
            logging.error(f"Error al ejecutar la consulta: {str(e)}")
            return {"statusCode": 500, "error": str(e)}



    def create_table(self, name_table, columns):
        table_creation_query = f"""
        CREATE TABLE IF NOT EXISTS {name_table} (
           {columns}
        );
        """
        redshift = Redshift()
        response = redshift.execute_query(table_creation_query)
        return response


    def load_table(self, create_table_redshift):        
        connection_cursor = self.connection.cursor()
        copy_sql = f"""
            {create_table_redshift}
        """
        print("imprime copy sql", copy_sql)
        connection_cursor.execute(copy_sql)
        self.connection.commit()
        connection_cursor.close()
        self.connection.close()


    def read_file_env(self):
        file = open(self.path_file, 'r')
        env_data = {}
        for line in file:
            if not line.strip() or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            env_data[key] = value
        return env_data





if __name__ == "__main__":
    BUCKET_NAME = 'redshift-cenam-claro'
    S3_KEY = 'redshift/datos.csv'
    FILE_NAME = 'datos.csv'
    REDSHIFT_HOST = 'kopicloud-workgroup.015319782619.us-east-1.redshift-serverless.amazonaws.com'
    REDSHIFT_DB = 'kopiclouddb'
    REDSHIFT_USER = 'kopiadmin'
    REDSHIFT_PASSWORD = 'M3ss1G0at10'
    REDSHIFT_PORT = 5439
    IAM_ROLE = 'arn:aws:iam::015319782619:role/kopicloud-dev-redshift-serverless-role-new'
    TABLE_NAME = 'users_t'


