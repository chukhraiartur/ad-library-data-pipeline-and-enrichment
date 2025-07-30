FROM apache/airflow:2.9.1-python3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Set working directory
WORKDIR /opt/airflow

# Copy project files
COPY --chown=airflow:root pyproject.toml poetry.lock ./

# Install Poetry and project dependencies
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --only main

# Copy source code
COPY --chown=airflow:root . .

# Set Python path
ENV PYTHONPATH=/opt/airflow

# Create necessary directories
RUN mkdir -p /opt/airflow/logs /opt/airflow/data/bronze /opt/airflow/data/silver /opt/airflow/data/gold

# Initialize Airflow database and create admin user
# Note: Default values are required for Docker build time
# At runtime, values from .env file will override these defaults
RUN airflow db init && \
    airflow users create \
    --username ${AIRFLOW_ADMIN_USER:-admin} \
    --firstname ${AIRFLOW_ADMIN_FIRSTNAME:-Admin} \
    --lastname ${AIRFLOW_ADMIN_LASTNAME:-User} \
    --role Admin \
    --email ${AIRFLOW_ADMIN_EMAIL:-admin@example.com} \
    --password ${AIRFLOW_ADMIN_PASSWORD:-admin}

# Expose port
EXPOSE 8080

# Default command
CMD ["airflow", "webserver"] 