import psycopg2
from psycopg2 import OperationalError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan



from setting import get_variables
from logger import *
import pandas as pd
from datetime import datetime, timedelta

class ElasticsearchToPostgres(Logger):
    def __init__(self, logfile):
        super().__init__(logfile)

        # SOURCE DB [ES]
        self.es_host = get_variables().ES_HOST
        self.es_port = get_variables().ES_PORT
        self.es_username = get_variables().ES_USER
        self.es_password = get_variables().ES_PASSWORD
        self.batch_size=int(get_variables().BATCH_SIZE)

        # TARGET DB [PG]
        self.pg_host = get_variables().PG_HOST
        self.pg_port = get_variables().PG_PORT
        self.pg_database = get_variables().PG_DATABASE
        self.pg_username= get_variables().PG_USER
        self.pg_password = get_variables().PG_PASSWORD
        self.pg_schema = get_variables().PG_SCHEMA

        # ETL DB [PG]
        self.etl_pg_host = get_variables().ETL_PG_HOST
        self.etl_pg_port = get_variables().ETL_PG_PORT
        self.etl_pg_database = get_variables().ETL_PG_DATABASE
        self.etl_pg_username= get_variables().ETL_PG_USER
        self.etl_pg_password = get_variables().ETL_PG_PASSWORD
        self.etl_pg_schema = get_variables().ETL_PG_SCHEMA
        self.etl_pg_table = get_variables().ETL_PG_TABLE

    # ElasticSearch Connection method
    def es_connect(self):
        try:
            connection = any
            # asdjasdasda
            
            if (self.es_username and self.es_password):
                connection = Elasticsearch([{"host": self.es_host, "port": self.es_port}], http_auth=(self.es_username, self.es_password))
            else:
                connection = Elasticsearch(self.es_host, basic_auth=(self.es_username, self.es_password))
            
            return connection  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Disconnect from ES
    def es_disconnect(self):
        if self.es_connect():
            self.es_connect().close()

    # PostgreSQL connection
    def pg_connect(self):
        try:
            connection = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_username,
                password=self.pg_password,
                dbname=self.pg_database,
                connect_timeout = 3600
            )
            
            connection.autocommit=True

            return connection  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Disconnect from PG
    def pg_disconnect(self):
        if self.pg_connect():
            self.pg_connect().close()

    def pg_etl_connect(self):
        try:
            connection = psycopg2.connect(
                host=self.etl_pg_host,
                port=self.etl_pg_port,
                user=self.etl_pg_username,
                password=self.etl_pg_password,
                dbname=self.etl_pg_database,
                connect_timeout = 3600
            )
            
            connection.autocommit=True

            return connection  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Disconnect from PG
    def pg_etl_disconnect(self):
        if self.pg_etl_connect():
            self.pg_etl_connect().close()
    
    # Get ETL mapping
    def pg_get_etl_table(self):
        try:
            query=f"""SELECT trg_table_name, src_index_name, src_pk_col_name, src_timestamp_col_name, last_sync_timestamp
                FROM {self.etl_pg_schema}.{self.etl_pg_table}
                WHERE is_active IS TRUE
                ORDER BY trg_table_name"""

            self.log_info(f"Executing : {query}")
            print(f"Executing : {query}")

            conn = self.pg_etl_connect()
            df = pd.read_sql(query, conn)

            #self.connection.commit()
            conn.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            return df  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    # Fetch Last Sync DateTime
    def fetch_last_sync_time(self, pg_table_name):
        try:
            query = f"""SELECT last_sync_timestamp FROM {self.etl_pg_schema}.{self.etl_pg_table} 
            WHERE trg_table_name='{pg_table_name}'"""
            
            print(f"Executing : {query}")
            self.log_info(f"Executing : {query}")

            cursor = self.pg_etl_connect().cursor()
            cursor.execute(query=query)
            result = cursor.fetchone()

            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")

            return result[0] if result else None

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    # Update last sync datetime
    def update_last_sync_time(self, pg_table_name, last_sync_time):
        try:
            query=f"""UPDATE {self.etl_pg_schema}.{self.etl_pg_table} SET
            last_sync_timestamp='{last_sync_time}'
            WHERE trg_table_name='{pg_table_name}' 
            """
            print(f"Executing : {query}")
            self.log_info(f"Executing : {query}")

            cursor = self.pg_etl_connect().cursor()
            cursor.execute(query=query)
            cursor.close()

            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")

            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
        
    # Execute PG DDL Statement
    def pg_execute_ddl(self, query):
        try:
            print(f"Executing : {query}")
            self.log_info(f"Executing : {query}")
            cursor = self.pg_connect().cursor()
            cursor.execute(query)
            #self.connection.commit()
            cursor.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            return True  # Success
        except OperationalError as e:
            print(f"DDL Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error
    
    def pg_execute_dml(self, query, value):
        try:
            #self.log_info(f"Executing : {query}")
            #print(f"Executing : {query}")
            cursor = self.pg_connect().cursor()
            if value is None:
                cursor.execute(query)
            else:
                cursor.execute(query, value)

            #self.connection.commit()
            cursor.close()
            #self.log_info(f"Executed : {query}")
            #print(f"Executed : {query}")
            return True  # Success
        except OperationalError as e:
            cursor.close()
            print(f"DML Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error
    
    # Get records of SELECT query
    def pg_get_df(self, query):
        try:
            self.log_info(f"Executing : {query}")
            print(f"Executing : {query}")

            conn = self.pg_connect()
            df = pd.read_sql(query, conn)

            #self.connection.commit()
            conn.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            return df  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error    
    
    # Create a PostgreSQL table based on Elasticsearch index mappings.
    def create_table_if_not_exists(self, mappings, pg_table_name):
        try:
            table_name=pg_table_name
            #table_name = index.replace('.', '_')
            columns = []

            print(f"Creating Table in PG : {table_name}")
            self.log_info(f"Creating Table in PG : {table_name}")

            for field, props in mappings.items():
                es_type = props.get('type', 'text')
                if es_type == 'text' or es_type == 'keyword':
                    pg_type = 'TEXT'
                elif es_type == 'integer':
                    pg_type = 'TEXT'
                elif es_type == 'long':
                    pg_type = 'TEXT'
                elif es_type == 'float':
                    pg_type = 'TEXT'
                elif es_type == 'TEXT':
                    pg_type = 'TEXT'
                elif es_type == 'boolean':
                    pg_type = 'TEXT'
                elif es_type == 'date':
                    pg_type = 'TEXT'
                else:
                    pg_type = 'TEXT'  # Default to TEXT for unsupported types

                columns.append(f'"{field}" {pg_type}')

            columns_def = ', '.join(columns)
            create_stmt = f'CREATE TABLE IF NOT EXISTS "{self.pg_schema}"."{table_name}" ({columns_def})'
            is_created_table=self.pg_execute_ddl(create_stmt)
   
            if is_created_table:
                print(f"Created Table in PG : {table_name}")
                self.log_info(f"Created Table in PG : {table_name}")

            return table_name
    
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Create Primary key if does not exists
    def create_primary_key(self, table_name, column_name):
        try:
            query = f"""
                SELECT 1 as cnt
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                WHERE tc.table_name = '{table_name}'
                AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                AND ccu.column_name = '{column_name}'
            ;"""
            df = self.pg_get_df(query=query)
            record_count = len(df)


            print(f"Record count: {record_count}")

            # Check primary key column is in UK
            if (record_count<1):

                # Check whether unique key index is existed or not
                index_name=f"uk_{table_name}_{column_name}"
                sql_uk_existance=f"""
                    SELECT 1 AS cnt
                    FROM pg_indexes
                    WHERE tablename = '{table_name}'
                    AND indexname = '{index_name}';
                    """
                
                df_uk = self.pg_get_df(query=sql_uk_existance)
                record_count_uk = len(df_uk)

                if(record_count_uk<1):
                    alter_query = f'ALTER TABLE {self.pg_schema}.{table_name} ADD CONSTRAINT {index_name} UNIQUE ("{column_name}");'
                    result = self.pg_execute_ddl(query=alter_query)

                    if (result):
                        print(f"Primary Key constraints is created on {table_name} using {column_name}.")
                        self.log_info(f"Primary Key constraints is created on {table_name} using {column_name}.")
                    else:
                        raise Exception(f"Unable to create primary key constraints on {table_name} using {column_name}.")

            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Fetch ElasticSearch Data
    def fetch_elasticsearch_data(self, index, timestamp_col_name, last_sync_ts):
        try:
            query = {"range": {f"{timestamp_col_name}": {"gt": last_sync_ts}}} if last_sync_ts else {"match_all": {}}
            print(f"Executing: {query}")
            self.log_info(f"Executing: {query}")
            return scan(self.es_connect(), index=index, query={"query": query}, size=self.batch_size)
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Upsert Records Into PostgreSQL
    def upsert_to_postgresql(self, table, primary_key_col, records):
        try:

            self.log_info("UPSERT")
            if not records:
                return True

            self.log_info(print(records[0]))
            print(records[0])

            columns = records[0].keys()
            values = [tuple(record[col] for col in columns) for record in records]
            placeholders = ", ".join(["%s"] * len(columns))
            
            # upd_fields = [k for k in columns if k != primary_key_col]
            # updates = ", ".join(f'"{field}" = EXCLUDED."{field}"' for field in upd_fields)
            
            insert_query = f"""
                INSERT INTO {self.pg_schema}.{table} ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT f"{primary_key_col}" DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key_col])}
            """
            self.log_info(insert_query)
            result = self.pg_execute_dml(query=insert_query, value=values)

            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    # List ES indexed excluding system indexes
    def get_es_indexes(self):
        try:
            indexes = [idx for idx in self.es_connect().indices.get_alias().keys() if not idx.startswith('.')]

            return indexes
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

     # Get ES index name as per etl table
    def get_es_index_name(self, index_list, index_to_find):
        try:
            return index_to_find if index_to_find in index_list else None

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
            
    def migrate_index(self, es_index, timestamp_col_name, pg_table, pk_col_name, last_sync_ts):
        try:
            total_processed = 0

            for batch in self.fetch_elasticsearch_data(es_index, timestamp_col_name, last_sync_ts):
                records = [
                {key: doc['_source'][key] for key in doc['_source']} 
                for doc in batch if '_source' in doc
                ]
             
                self.upsert_to_postgresql(pg_table, pk_col_name, records)
                total_processed += len(records)

            return total_processed
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    def run_migration(self):
        try:
            status = True
            #Get ETL Mapping
            mappings = self.pg_get_etl_table()
            # Get Index List
            index_list=self.get_es_indexes()
            
            for key, row in mappings.iterrows():
                pg_table_name = row['trg_table_name']
                es_index_name = row['src_index_name']
                pk_col_name = row['src_pk_col_name']
                timestamp_col_name=row['src_timestamp_col_name']
                last_sync_timestamp=row['last_sync_timestamp']

                # Check if index exist or not
                es_index = self.get_es_index_name(index_list=index_list, index_to_find=es_index_name)

                # Get current datetime and minus 15 minutes
                new_sync_time = datetime.now() - timedelta(minutes=15)

                if (es_index==None):
                    raise Exception(f"Invalid index: {es_index_name} does not exist.")

                print(f"Starting migration for index: {es_index} -> table: {pg_table_name}")
                self.log_info(f"Starting migration for index: {es_index} -> table: {pg_table_name}")
                
                processed = self.migrate_index(es_index, timestamp_col_name, pg_table_name, pk_col_name, last_sync_timestamp)
                
                print(f"Processed {processed} records for index {es_index}.")
                self.log_info(f"Processed {processed} records for index {es_index}.")
                

                if processed:
                    self.update_last_sync_time(last_sync_time=new_sync_time, pg_table_name=pg_table_name)
                else:
                        print(f"Error: Unable to migrate the {es_index} index.")
                        self.log_error(f"Unable to migrate the {es_index} index.")
            status=True
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            status=None  # Error

        finally:
            # Close PostgreSQL connection
            self.pg_disconnect()
            self.pg_etl_disconnect()
            self.es_disconnect()
            return status
            