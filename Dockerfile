# Use the official Python runtime as a parent image
#FROM python:3.9-slim
FROM python:3.11

# Set the time zone
ENV TZ=Asia/Dhakado

# Install the tzdata package to set the time zone
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*

# Set the system time zone
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Remove log directory from /app
RUN rm -rf /app/logs
RUN rm -rf /app/cred
RUN rm -rf /app/config

# Define a volume mount point
VOLUME /app/logs
VOLUME /app/cred
VOLUME /app/config

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x app.py

# Run app.py when the container launches
CMD ["python", "app.py"]