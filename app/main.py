print('--- Start ---')

import pandas as pd
import psycopg2.extras as extras
import psycopg2
import time
import os

# docker-compose build --no-cache && docker-compose up -d --force-recreate

CWD = os.path.dirname(os.path.abspath(__file__))

def main():
    PATH_2019 = f'{CWD}/data/Odata2019File.csv'
    PATH_2020 = f'{CWD}/data/Odata2020File.csv'

    DTYPE_DICT = {
        'int64': 'INT',
        'float64': 'NUMERIC',
        'object': 'VARCHAR(256)'
    }

    def create_table(conn, df, table):
        # Creating table
        data_types = df.dtypes.apply(lambda x: str(x))
        data_types = data_types.apply(lambda x: DTYPE_DICT[x])

        cur = conn.cursor()

        # DROP TABLE IF EXISTS {table};
        create_table_query = \
        f'''
        CREATE TABLE IF NOT EXISTS {table}(
            {' '.join([f'{col} {data_type},' for col, data_type in zip(data_types.index, data_types)])}
        '''
        create_table_query = create_table_query.strip()[:-1] + ');'

        cur.execute(create_table_query)
        conn.commit()

        # Inserting into table
        tuples = [tuple(x) for x in df.to_numpy()]
    
        cols = ','.join(list(df.columns))

        insert_query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        
        df_len = len(tuples)
        print(f'Must be inserted: {df_len} records')

        check_count_query = "SELECT COUNT(*) FROM %s" % (table)

        cur.execute(check_count_query)
        table_len = cur.fetchone()[0]
        print(f'Already inserted: {table_len} records')

        if table_len != df_len:
            print('Inserting...')

        i = 1
        for row in range(table_len, df_len):
            try:
                extras.execute_values(cur, insert_query, [tuples[row]])
                conn.commit()
                
                i += 1

            except Exception as e:
                error_string = str(e)
                print(f'Error: {error_string}')
                print(f'Inserted {i} records.')
                print('Restarting insertion...')
                if error_string == 'cursor already closed':
                    print('Connection to database is lost. Reconnecting...')
                    while True:
                        try:
                            conn = psycopg2.connect(
                                # database="db_01_shevtsov", user='postgres', password='133451', host='127.0.0.1', port='5432'
                                database="db_01_shevtsov", user='postgres', password='133451', host='database', port='5432'        
                            )
                            cur = conn.cursor()

                            print('Connection to database is succesful. Insertion is continuing...')
                            break
                        except:
                            time.sleep(5)
                            continue

        conn.commit()

        check_distinct_count_query = "SELECT COUNT(*) FROM %s" % (table)
        cur.execute(check_distinct_count_query)
        distinct_table_len = cur.fetchone()[0]
        print(f'There are {distinct_table_len} records in table.')

        print(f'Table {table} is created and filled.')

        cur.close()
        return conn

    print('Reading zno_2019 csv file...')
    zno_2019_df = pd.read_csv(PATH_2019, sep=';', encoding='windows-1251', low_memory=False)
    zno_2019_df = zno_2019_df[['REGNAME', 'physBall100', 'physTestStatus']]
    zno_2019_df.columns = ['REGNAME_2019', 'physBall100_2019', 'physTestStatus_2019']

    print('Reading zno_2020 csv file...')
    zno_2020_df = pd.read_csv(PATH_2020, sep=';', encoding='windows-1251', low_memory=False)
    zno_2020_df = zno_2020_df[['REGNAME', 'physBall100', 'physTestStatus']]
    zno_2020_df.columns = ['REGNAME_2020', 'physBall100_2020', 'physTestStatus_2020']
    
    zno_phys_records_2019_2020 = pd.concat([zno_2019_df, zno_2020_df], axis=1)
    
    conn = psycopg2.connect(
        # database="db_01_shevtsov", user='postgres', password='133451', host='127.0.0.1', port='5432'
        database="db_01_shevtsov", user='postgres', password='133451', host='database', port='5432'        
    )
    
    cur = conn.cursor()

    print('Creating zno_phys_records_2019_2020 table...')
    conn = create_table(conn, zno_phys_records_2019_2020, 'zno_phys_records_2019_2020')

    cur = conn.cursor()

    # 11. Порівняти середній бал з Фізики у кожному регіоні у 2020 та 2019 роках серед тих кому
    # було зараховано тест

    avg_phys_grades_query = \
    '''
    SELECT z_2019.Регіон, Середній_бал_з_фізики_2019, Середній_бал_з_фізики_2020
    FROM (
        SELECT REGNAME_2019 AS Регіон,
            ROUND(AVG(REPLACE(physBall100_2019, ',', '.')::NUMERIC), 2) AS Середній_бал_з_фізики_2019
        FROM zno_phys_records_2019_2020
        WHERE physTestStatus_2019 = 'Зараховано' AND
            physBall100_2019 <> 'NaN'
        GROUP BY 1
        ORDER BY 1
    ) z_2019 JOIN
    (
        SELECT REGNAME_2020 AS Регіон,
            ROUND(AVG(REPLACE(physBall100_2020, ',', '.')::NUMERIC), 2) AS Середній_бал_з_фізики_2020
        FROM zno_phys_records_2019_2020
        WHERE physTestStatus_2020 = 'Зараховано' AND
            physBall100_2020 <> 'NaN'
        GROUP BY 1
        ORDER BY 1
    ) z_2020 ON z_2019.Регіон = z_2020.Регіон
    '''

    print('Executing average physics grades query...')
    cur.execute(avg_phys_grades_query)

    avg_phys_grades_df = pd.DataFrame(cur.fetchall())
    avg_phys_grades_df.columns = ['Регіон', 'Середній_бал_з_фізики_2019', 'Середній_бал_з_фізики_2020']
    avg_phys_grades_df.to_csv(f'{CWD}/result.csv', encoding='windows-1251', index=False)

start_time = time.time()

main()

elapsed_time = round(time.time() - start_time, 2)

with open(f'{CWD}/time.txt', 'w') as f:

    f.write(f'Elapsed time in seconds: {elapsed_time}')

print('--- Finish ---')