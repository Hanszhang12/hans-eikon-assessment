import subprocess

commands = [
    "docker build -t flask_image .",
    "docker network create shared_network",
    "docker run -d -p 8000:8000 --name flask_container --net shared_network flask_image",
    "docker run -d --name postgres_container -e POSTGRES_USER=defaultuser -e POSTGRES_PASSWORD=defaultpassword -e POSTGRES_DB=postgres -p 5432:5432 --net shared_network postgres"
]

# Function to run commands
def run_commands(commands_list):
    for cmd in commands_list:
        try:
            print(f"Running command: {cmd}")
            subprocess.run(cmd, shell=True, check=True)
            print("Command completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error running command: {cmd}")
            print(f"Error message: {e}")
            break

if __name__ == "__main__":
    run_commands(commands)
