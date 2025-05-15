# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED True  # Ensures logs are sent out immediately
ENV APP_HOME /app          # Define a home directory for the app
WORKDIR $APP_HOME          # Set the working directory

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the local Flask app code to the container
COPY app.py .

# Specify the command to run on container start
# Gunicorn will serve the app. 'app:app' means look for an object named 'app' in a file named 'app.py'
# $PORT is automatically set by Cloud Run.
CMD exec gunicorn --bind "0.0.0.0:$PORT" app:app