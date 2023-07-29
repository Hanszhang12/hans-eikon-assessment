import csv
import psycopg2
from psycopg2 import OperationalError
from flask import Flask
import time

def etl():
    compounds_csv = "data/compounds.csv"
    user_experiments_csv = "data/user_experiments.csv"
    users_csv = "data/users.csv"

    csv_files = [compounds_csv, user_experiments_csv, users_csv]
    data = {}

    for file in csv_files:
        new_data = []
        with open(file, "r") as csvfile:
                csvreader = csv.reader(csvfile)
                for row in csvreader:
                    new_data.append(row)
        data[file] = new_data


    #map compound id to compound name
    compound_id_to_name = {}

    #map user id to user name
    user_id_to_name = {}

    #map user id to count of each compound
    user_id_to_compound_count = {}

    #dict to keep track of compound count
    compound_count = {}
    for row in data[compounds_csv][1:]:
        compound_id_to_name[row[0]] = row[1]
        compound_count[row[0]] = 0

    #for each user id, we map the user id to user name
    #we also initialize the compound counts for each user id to 0
    for user in data[users_csv][1:]:
        user_id = user[0]
        user_name = user[1]
        user_id_to_name[user_id] = user_name

        #map existing compounds to userID
        user_id_to_compound_count[user_id] = compound_count.copy()


    #count number of experiments for each user id
    user_name_to_exp_count = {}
    total_experiment_count = 0

    for experiments in data[user_experiments_csv][1:]:
        user_id = experiments[1]
        user_name = user_id_to_name[user_id]
        user_name_to_exp_count[user_name] = user_name_to_exp_count.get(user_name, 0) + 1
        total_experiment_count += 1

        #get the compounds
        experiment_compounds = experiments[2].split(';')

        #loop through the compounds used in this experiment and add to the count for the userID
        for compound_id in experiment_compounds:
            user_id_to_compound_count[user_id][compound_id] += 1

    #calculate most commonly used compound for each user
    user_name_to_most_common = {}
    for user_id in user_id_to_compound_count:
        max_count = 0
        common_compound = None

        for compound_id in user_id_to_compound_count[user_id]:
            curr_count = user_id_to_compound_count[user_id][compound_id]
            if curr_count > max_count:
                max_count = curr_count
                common_compound = compound_id_to_name[compound_id]
            elif curr_count == max_count and curr_count != 0:
                common_compound = common_compound + ";" + compound_id_to_name[compound_id]

        user_name = user_id_to_name[user_id]
        if common_compound:
            user_name_to_most_common[user_name] = common_compound
        else:
            user_name_to_most_common[user_name] = None

    #calculate average experiments per user
    avg_experiments = total_experiment_count/len(user_id_to_name)
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
