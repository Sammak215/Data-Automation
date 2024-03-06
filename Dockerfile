# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port that the app will run on
EXPOSE 8080

# Define environment variable
ENV UVICORN_CMD "uvicorn main:app --host=0.0.0.0 --port=8080 --reload"

# Run the application when the container launches
CMD ["bash", "-c", "$UVICORN_CMD"]
