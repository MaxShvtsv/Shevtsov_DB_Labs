print('--- Start ---')

import psycopg2.extras as extras
import pandas as pd
import psycopg2
import time
import os

# docker-compose build --no-cache && docker-compose up -d --force-recreate
# psql -U postgres -d db_01_shevtsov
# \dt

CWD = os.path.dirname(os.path.abspath(__file__))

PATH_2019 = f'{CWD}/data/Odata2019File.csv'
PATH_2020 = f'{CWD}/data/Odata2020File.csv'

DTYPE_DICT = {
    'int64': 'INT',
    'float64': 'NUMERIC',
    'object': 'VARCHAR(256)'
}

def connect_to_database():
    my_host = 'database'
    # my_host = '127.0.0.1'

    return psycopg2.connect(
        database="db_01_shevtsov", user='postgres', password='133451', host=my_host, port='5432'
    )

def creating_tables(conn, df, table):

    print(f'Creating {table}...')

    cur = conn.cursor()

    data_types = df.dtypes.apply(lambda x: str(x))
    data_types = data_types.apply(lambda x: DTYPE_DICT[x])

    create_table_query = \
    f'''
    DROP TABLE IF EXISTS {table};
    CREATE TABLE {table}(
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

    if table_len < df_len:
        print('Inserting...')

    i = 1
    for row in range(table_len, df_len):
        try:
            extras.execute_values(cur, insert_query, [tuples[row]])

            conn.commit()

            if i % 1000 == 0:
                print(f'Inserted {i} records.')

            # Will be inserted only 10000 records!!!
            if i % 10000 == 0:
                break

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
                        conn = connect_to_database()
                        cur = conn.cursor()

                        print('Connection to database is succesful. Insertion is continuing...')
                        break
                    except:
                        time.sleep(5)
                        continue

    check_distinct_count_query = "SELECT COUNT(*) FROM %s" % (table)
    cur.execute(check_distinct_count_query)
    distinct_table_len = cur.fetchone()[0]
    print(f'There are {distinct_table_len} records in table.')

    print(f'Table {table} is created and filled.')

    cur.close()

    return conn

# Starting point
def main():

    conn = connect_to_database()

    init_table_name = 'zno_records'

    print('Reading zno_2019 csv file...')
    zno_2019_df = pd.read_csv(PATH_2019, sep=';', encoding='windows-1251', low_memory=False)
    zno_2019_df['year'] = 2019

    print('Reading zno_2020 csv file...')
    zno_2020_df = pd.read_csv(PATH_2020, sep=';', encoding='windows-1251', low_memory=False)
    zno_2020_df['year'] = 2020
    
    zno_records = pd.concat([zno_2019_df, zno_2020_df])

    # Shuffle
    zno_records = zno_records.sample(frac=1)

    zno_records['student_id'] = list(range(1, len(zno_records) + 1))

    conn = creating_tables(conn, zno_records, init_table_name)

    cur = conn.cursor()

    # 11. Порівняти середній бал з Фізики у кожному регіоні у 2020 та 2019 роках серед тих кому
    # було зараховано тест

    while True:
        get_tables_query = \
        '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        '''

        cur.execute(get_tables_query)

        tables = cur.fetchall()
        have_to_be_tables = [('zno_locations',), ('schools',), ('results',), ('students',)]

        for table in have_to_be_tables:
            if table not in tables:
                print(f'Not found {table} table. Waiting for migration...')
                time.sleep(5)
                continue
            else:
                print(f'{table} is exists.')
        
        print('All needed tables are found.')

        print('Executing average physics grades query...')

        avg_phys_grades_query = \
        '''
        SELECT z_2019.Регіон, Середній_бал_з_фізики_2019, Середній_бал_з_фізики_2020
        FROM (
            SELECT ptregname AS Регіон, ROUND(AVG(REPLACE(ball100, ',', '.')::NUMERIC), 2) AS Середній_бал_з_фізики_2019
            FROM results
            INNER JOIN students USING(student_id)
            INNER JOIN zno_locations ON zno_locations.location_id = students.reg_location_id
            WHERE teststatus = 'Зараховано' AND
                ball100 <> 'NaN' AND
                testname = 'Фізика' AND
                year = 2019
            GROUP BY Регіон
        ) z_2019 JOIN
        (
            SELECT ptregname AS Регіон, ROUND(AVG(REPLACE(ball100, ',', '.')::NUMERIC), 2) AS Середній_бал_з_фізики_2020
            FROM results
            INNER JOIN students USING(student_id)
            INNER JOIN zno_locations ON zno_locations.location_id = students.reg_location_id
            WHERE teststatus = 'Зараховано' AND
                ball100 <> 'NaN' AND
                testname = 'Фізика' AND
                year = 2020
            GROUP BY Регіон
        ) z_2020 ON z_2019.Регіон = z_2020.Регіон;
        '''

        cur.execute(avg_phys_grades_query)

        avg_phys_grades_df = pd.DataFrame(cur.fetchall())
        avg_phys_grades_df.columns = ['Регіон', 'Середній_бал_з_фізики_2019', 'Середній_бал_з_фізики_2020']
        avg_phys_grades_df.to_csv(f'{CWD}/result.csv', encoding='windows-1251', index=False)

        print('Executed successfully.')
        break

start_time = time.time()

main()

elapsed_time = round(time.time() - start_time, 2)

with open(f'{CWD}/time.txt', 'w') as f:

    f.write(f'Elapsed time in seconds: {elapsed_time}')

print('--- Finish ---')