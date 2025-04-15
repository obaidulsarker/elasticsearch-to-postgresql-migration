from elasticsearch import Elasticsearch, NotFoundError
import json
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError
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

            if (self.es_username and self.es_password):
                connection = Elasticsearch([f"{self.es_host}"],
                                           basic_auth=(f"{self.es_username}", f"{self.es_password}"),
                                           verify_certs=False
                                            )
            elif self.es_host.startswith("https://"):
                connection = Elasticsearch(self.es_host,
                    verify_certs=False,  # ONLY for local testing. NEVER use in production!
                    ssl_assert_hostname=False,  # ONLY for local testing. NEVER use in production!
                    ssl_show_warn=False  # ONLY for local testing. NEVER use in production!
                )
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
    
    def pg_execute_dml_return_rowcount(self, query, value):
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
            return cursor.rowcount  # Success
        
        except OperationalError as e:
            cursor.close()
            print(f"DML Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return 0  # Error
        
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
        
    # Convert nested dictionaries or lists to JSON strings.
    def sanitize_data(self, source):
        try:
            sanitized = {}
            for key, value in source.items():
                if isinstance(value, (dict, list)):
                    sanitized[key] = json.dumps(value)
                else:
                    sanitized[key] = value
            return sanitized
        
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

            # Connection Obj
            #pg_connection= self.pg_connect()

            columns_def = ', '.join(columns)
            create_stmt = f'CREATE TABLE IF NOT EXISTS "{self.pg_schema}"."{table_name}" ({columns_def})'
            is_created_table=self.pg_execute_ddl(create_stmt)
            #pg_connection.cursor.execute(create_stmt)
            #pg_connection.commit()

            if is_created_table:
                print(f"Created Table in PG : {table_name}")
                self.log_info(f"Created Table in PG : {table_name}")

            return table_name
    
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
    
    # Fetches updated records from Elasticsearch
    def fetch_elasticsearch_data(self, index_name, updated_at):
        try:
            query = {
                "query": {
                    "range": {
                        "updatedAt": {
                            "gte": updated_at
                        }
                    }
                }
            }
            return self.es_connect().search(index=index_name, body=query)

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
        
    # Migrate data from an Elasticsearch index to a PostgreSQL table.
    def migrate_es_index_into_pg(self, index, pg_table_name, primary_key_col, timestamp_col_name,  last_sync_time):
        try:
            print(f"Starting migration for index: {index} -> table: {pg_table_name}")
            self.log_info(f"Starting migration for index: {index} -> table: {pg_table_name}")

            # ES Connection Object
            es=self.es_connect()

            # Get index mappings
            mappings = es.indices.get_mapping(index=index)[index]['mappings']['properties']

            # Create table in PostgreSQL
            table_name = self.create_table_if_not_exists(mappings=mappings, pg_table_name=pg_table_name)

            # Create Unique Index, if not existed
            create_index = self.create_primary_key(table_name=table_name, column_name=primary_key_col)

            if (create_index is None):
                raise Exception(f"Unable to create Unique Key Constraint on {table_name} using {primary_key_col} column")

            # Use Elasticsearch scroll API for large datasets
            query = {"range": {f"{timestamp_col_name}": {"gt": last_sync_time}}} if last_sync_time else {"match_all": {}}
            docs = es.search(index=index, query=query, scroll="5m", size= self.batch_size)
            #docs = es.searchindex=index, query=query, size=5000, scroll="5m"
            
            print(f"query={query}")
            rec_cnt =0

            #docs = es.search(index=index, body={"query": {"match_all": {}}}, size=1000, scroll="1m")
            scroll_id = docs['_scroll_id']
            hits = docs['hits']['hits']

            while hits:
                for hit in hits:
                    source = hit["_source"]
                    sanitized_source = self.sanitize_data(source)

                    columns = ', '.join([f'"{key}"' for key in sanitized_source.keys()])
                    #print(f"column : {columns}")
                    
                    upd_fields = [k for k in sanitized_source.keys() if k != primary_key_col]
                    updates = ", ".join(f'"{field}" = EXCLUDED."{field}"' for field in upd_fields)

                    values = tuple(sanitized_source.values())
                    #records_count = len(values)
                    placeholders = ', '.join(['%s'] * len(values))

                    insert_stmt = f"""INSERT INTO {self.pg_schema}.{table_name} ({columns}) 
                    VALUES ({placeholders}) 
                    ON CONFLICT ("{primary_key_col}") DO UPDATE SET {updates};
                    """

                    records_count = self.pg_execute_dml_return_rowcount(query=insert_stmt, value=values)
                    rec_cnt = rec_cnt + records_count
  
                # Fetch next batch of results
                docs = es.scroll(scroll_id=scroll_id, scroll="5m")
                scroll_id = docs['_scroll_id']
                hits = docs['hits']['hits']
                
            print(f"Total Records Processed: {rec_cnt}")
            self.log_info(f"Total Records Processed: {rec_cnt}")

            print(f"Finished migration for index: {index} -> table: {pg_table_name}")
            self.log_info(f"Finished migration for index: {index} -> table: {pg_table_name}")

            return True
        
        except NotFoundError as e:
            print(f"Scroll context not found: {e}")
            self.log_error(f"Scroll context not found: {e}")
            return True
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
        finally:
            if 'scroll_id' in locals():
                es.clear_scroll(scroll_id=scroll_id)


    def run_migration(self):
        try:
            status = None

            # Get Index List
            indexes=self.get_es_indexes()

            # ETL table list
            etl_list = self.pg_get_etl_table()
       
            for index, row in etl_list.iterrows():
                trg_table_name = row['trg_table_name']
                src_index_name = row['src_index_name']
                src_pk_col_name = row['src_pk_col_name']
                src_timestamp_col_name=row['src_timestamp_col_name']
                last_sync_timestamp=row['last_sync_timestamp']

                # Check if index exist or not
                index = self.get_es_index_name(index_list=indexes, index_to_find=src_index_name)
                
                # Get current datetime and minus 15 minutes
                new_sync_time = datetime.now() - timedelta(minutes=15)

                #if (self.create_primary_key(table_name=trg_table_name, column_name=src_pk_col_name)==True):
                    
                # Start Migration
                result = self.migrate_es_index_into_pg(index=index, last_sync_time=last_sync_timestamp, pg_table_name=trg_table_name, primary_key_col=src_pk_col_name, timestamp_col_name=src_timestamp_col_name)
                    
                # Update ETL
                if (result):
                    self.update_last_sync_time(last_sync_time=new_sync_time, pg_table_name=trg_table_name)
                else:
                    print(f"Error: Unable to migrate the {index} index.")
                    self.log_error(f"Unable to migrate the {index} index.")

            status= True
        
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
        
