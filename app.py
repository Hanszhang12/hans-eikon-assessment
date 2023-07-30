import csv
import psycopg2
from psycopg2 import OperationalError
from flask import Flask
import time

def read_csv_file(file_path):
    with open(file_path, "r") as csvfile:
        csvreader = csv.reader(csvfile)
        return list(csvreader)

def etl():
    compounds_csv = "data/compounds.csv"
    user_experiments_csv = "data/user_experiments.csv"
    users_csv = "data/users.csv"

    csv_files = [compounds_csv, user_experiments_csv, users_csv]
    data = {file: read_csv_file(file) for file in csv_files}

    compound_id_to_name = {row[0]: row[1] for row in data[compounds_csv][1:]}
    user_id_to_name = {user[0]: user[1] for user in data[users_csv][1:]}

    user_id_to_compound_count = {user_id: {comp_id: 0 for comp_id in compound_id_to_name} for user_id in user_id_to_name}

    user_name_to_exp_count = {}
    total_experiment_count = 0

    for experiments in data[user_experiments_csv][1:]:
        user_id, _, compound_ids = experiments[1], experiments[0], experiments[2].split(';')
        user_name = user_id_to_name[user_id]

        user_name_to_exp_count[user_name] = user_name_to_exp_count.get(user_name, 0) + 1
        total_experiment_count += 1

        for compound_id in compound_ids:
            user_id_to_compound_count[user_id][compound_id] += 1

    user_name_to_most_common = {}
    for user_id, compound_counts in user_id_to_compound_count.items():
        max_count = 0
        common_compound = []

        for compound_id, count in compound_counts.items():
            if count > max_count:
                max_count = count
                common_compound = [compound_id_to_name[compound_id]]
            elif count == max_count and count != 0:
                common_compound.append(compound_id_to_name[compound_id])

        user_name = user_id_to_name[user_id]
        user_name_to_most_common[user_name] = ';'.join(common_compound) if common_compound else None

    avg_experiments = total_experiment_count / len(user_id_to_name)
    
    connection = None
    while not connection:
        try:
            connection = psycopg2.connect(
                host="postgres_container",
                database="postgres",
                user="defaultuser",
                password="defaultpassword",
                port=5432
            )
        except OperationalError as e:
            print(e)
            time.sleep(5)
    cursor = connection.cursor()

    # create table users
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            user_name VARCHAR NOT NULL,
            total_experiments INTEGER NOT NULL,
            common_compound VARCHAR NOT NULL,
            average_experiments FLOAT NOT NULL
        )
    '''
    cursor.execute(create_table_query)

    insert_data_query = '''
    INSERT INTO users (user_name, total_experiments, common_compound, average_experiments)
    VALUES (%s, %s, %s, %s)
    '''

    data_to_insert = []

    #get the data we want to insert
    for user_id in user_id_to_name:
        user = user_id_to_name[user_id]

        new_row = []
        new_row.append(user)

        if user in user_name_to_exp_count:
            new_row.append(user_name_to_exp_count[user])
        else:
            new_row.append(0)

        if user_name_to_most_common[user]:
            new_row.append(user_name_to_most_common[user])
        else:
            new_row.append("No experiments were performed by user")

        new_row.append(avg_experiments)

        data_to_insert.append(new_row)

    cursor.executemany(insert_data_query, data_to_insert)

    connection.commit()

    cursor.close()
    connection.close()

    return 'ETL process successful'


app = Flask(__name__)

# Your API that can be called to trigger your ETL process
@app.route('/trigger-etl', methods=['GET'])
def trigger_etl():
    # Trigger your ETL process here
    result = etl()

    return {"message": result}, 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
