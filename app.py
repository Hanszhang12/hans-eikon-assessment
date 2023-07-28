import csv
import psycopg2

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

    #for each user id, we map the user id to user user name
    #we also initialize the compound counts for each user id to 0
    for user in data[users_csv][1:]:
        user_id = user[0]
        user_name = user[1]
        user_id_to_name[user_id] = user_name

        #map existing compounds to userID
        user_id_to_compound_count[user_id] = compound_count.copy()


    #count number of experiments for each user id
    user_id_to_exp_count = {}
    total_experiment_count = 0

    for experiments in data[user_experiments_csv][1:]:
        user_id = experiments[1]
        user_id_to_exp_count[user_id] = user_id_to_exp_count.get(user_id, 0) + 1
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
                common_compound = compound_id

        #grab user name using user id
        user_name = user_id_to_name[user_id]
        if common_compound:
            compound_name = compound_id_to_name[common_compound]
            user_name_to_most_common[user_name] = compound_name
        else:
            user_name_to_most_common[user_name] = None

    #calculate average experiments per user
    avg_experiments = total_experiment_count/len(user_id_to_name)

    for student in user_id_to_exp_count:
        print(user_id_to_name[student], ":", user_id_to_exp_count[student])

    print("Average Experiments Amount per User: ", avg_experiments)

    #if user's most common compound is None, that means no experiments were performed
    print("Most common compounds:", user_name_to_most_common)


# Your API that can be called to trigger your ETL process
def trigger_etl():
    # Trigger your ETL process here
    conn = psycopg2.connect(
        database="exampledb",
        user="docker",
        password="docker",
        host="0.0.0.0",
        port="5433"
    )
    # cur = conn.cursor()
    etl()

    # cur.close()
    conn.close()
    return {"message": "ETL process started"}, 200
    
trigger_etl()
