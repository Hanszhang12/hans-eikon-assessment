# Use the official Python base image with version 3.9
FROM python:3.9

# Set the working directory inside the container
WORKDIR app

# Copy the application files to the working directory
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir psycopg2

# Run the Python application
CMD ["python", "app.py"]
