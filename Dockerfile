# Base image
FROM apache/airflow:2.10.4
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip uninstall multipart -y

USER root
# Install system dependencies
RUN apt-get update && apt-get install -y \
  build-essential \
  curl \
  wget \
  git \
  unzip \
  libpq-dev \
  libssl-dev \
  libffi-dev \
  gcc \
  g++ \
  && apt-get clean

USER airflow

# # Create virtual environment
# RUN python -m venv /workspace/venv

# # Set environment variables
# ENV PATH="/workspace/venv/bin:$PATH"

# # Activate virtual environment
# RUN echo "source /workspace/venv/bin/activate" >> ~/.bashrc

# # Install Python libraries
# COPY requirements.txt /tmp/
# RUN pip install --upgrade pip && pip install -r /tmp/requirements.txt

# RUN pip3 install flask-session

# # Run entrypoint script
# COPY entrypoint.sh /usr/local/bin/
# RUN chmod +x /usr/local/bin/entrypoint.sh
# RUN . /usr/local/bin/entrypoint.sh

# RUN . ~/.bashrc

# # Expose necessary ports
EXPOSE 3000 5000 8080 1234

# ENV AIRFLOW_HOME=/opt/airflow

# # # Default command
# # CMD [ "bash" ]
# COPY airflow.sh /usr/local/bin/
# RUN chmod +x /usr/local/bin/airflow.sh

# ENTRYPOINT [ "airflow.sh" ]
