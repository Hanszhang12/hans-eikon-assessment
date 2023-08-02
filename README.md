# Python ETL Pipeline

### Introduction
In this challenge, you will be tasked with creating a simple ETL pipeline that can be triggered via an API call. You will be provided with a set of CSV files that you will need to process, derive some features from, and then upload into a database table.

### Requirements
- Python 3.7+
- Docker
- PostgreSQL

### Challenge
1.  Create a Dockerized application that can be started with a single `docker run` command.

2. The application should expose an API endpoint that triggers an ETL process.

3. The ETL process should:
- Load CSV files from the given data directory.
 - Process these files to derive some simple features.
 - Upload the processed data into a **postgres** table.

4.  The application should be built using Python and any tooling you like for coordinating the workflow and fronting the api server

### Data
You will find three CSV files in the `data`  directory:

- `users.csv`: Contains user data with the following columns: `user_id`, `name`, `email`,`signup_date`.

- `user_experiments.csv`: Contains experiment data with the following columns: `experiment_id`, `user_id`, `experiment_compound_ids`, `experiment_run_time`. The `experiment_compound_ids` column contains a semicolon-separated list of compound IDs.


- `compounds.csv`: Contains compound data with the following columns: `compound_id`, `compound_name`, `compound_structure`.


## Feature Derivation
From the provided CSV files, derive the following features:

1. Total experiments a user ran.
2. Average experiments amount per user.
3. User's most commonly experimented compound.

## Deliverables
Please provide the following in a GITHUB REPOSITORY.

1. A Dockerfile that sets up the environment for your application.
2. A requirements.txt file with all the Python dependencies.
3. A Python script that sets up the API and the ETL process.
4. A brief README explaining how to build and run your application, and how to trigger the ETL process.


Please also provide a script that builds, and runs the docker container.
You should also provide a script that scaffolds how a user can run the ETL process. This can be `curl` or something else.
Finally, provide a script that queries the database and showcases that it has been populated with the desired features.


## How to run the code
```
git clone https://github.com/Hanszhang12/hans-eikon-assessment.git
cd hans-eikon-assessment

#Build and run docker containers
python3 build.py

#Trigger the ETL pipeline
curl http://localhost:8000/trigger-etl

#Connect to postgres database as default user
docker exec -it postgres_container psql -U defaultuser -d postgres

#Run this sql query to see the data in users table
SELECT * from users;

#The table should look something like this
id | user_name | total_experiments |         common_compound          | average_experiments 
----+-----------+-------------------+----------------------------------+---------------------
  1 | Alice     |                 2 | Compound B                       |        1.1
  2 | Bob       |                 1 | Compound A;Compound C            |        1.1
  3 | Carol     |                 1 | Compound B;Compound C            |        1.1
  4 | Dave      |                 1 | Compound A;Compound B;Compound C |        1.1
  5 | Eve       |                 1 | Compound B;Compound C            |        1.1
  6 | Frank     |                 1 | Compound A;Compound C            |        1.1
  7 | Grace     |                 1 | Compound B;Compound C            |        1.1
  8 | Heidi     |                 1 | Compound A;Compound B            |        1.1
  9 | Ivan      |                 1 | Compound B;Compound C            |        1.1
 10 | Judy      |                 1 | Compound A;Compound C            |        1.1
```
