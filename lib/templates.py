# -*- coding: utf-8 -*-

DESCRIBE_TABLE = """
                    describe {table_name};
                 """
SELECT_MAX = """
                SELECT max({column_name}) as max FROM {dataset}.{table_name}
             """ 

COUNT_DIFF = """
                SELECT count({column_name}) as 'count' FROM {table_name} WHERE {column_name} > {last_id};
             """ 


DUMP_TABLE = """
                SELECT {column_names}

                UNION ALL
                SELECT {column_query}
                INTO OUTFILE "{file}"
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                FROM {table_name}
                {where}
            """