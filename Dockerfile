# Use Miniconda base image
FROM continuumio/miniconda3:latest

# Set working directory
WORKDIR /app

# Copy the local environment file and the google-cloud-secret-manager .tar file
COPY environment.yml /app/environment.yml
COPY google_cloud_secret_manager-2.20.2.tar /app/google_cloud_secret_manager-2.20.2.tar

# Install conda dependencies
RUN conda env create -f /app/environment.yml

# Ensure the conda environment is activated and accessible
SHELL ["conda", "run", "-n", "new-env", "/bin/bash", "-c"]

# Install google-cloud-secret-manager from the .tar file and functions-framework using pip
RUN pip install /app/google_cloud_secret_manager-2.20.2.tar \
    && pip install functions-framework

# Ensure the environment is activated at runtime (fixed format)
ENV PATH="/opt/conda/envs/new-env/bin:$PATH"

# Copy the rest of the application to the container
COPY . /app

# Expose port 8080
EXPOSE 8080

# Run the application with Functions Framework
# CMD ["functions-framework", "--target=task", "--port=8080"]
# CMD ["functions-framework", "--source=functions/schema-setup/main.py", "--target=task", "--port=8080"]
# CMD ["functions-framework", "--source=flow/daily_update.py", "--target=daily_update", "--port=8080"]

# Dockerfile for schema-setup
CMD ["functions-framework", "--source=functions/schema-setup/main.py", "--target=schema_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy schema-service --image gcr.io/ba882-rgk/my-container --platform managed

# Dockerfile for extract
# CMD ["functions-framework", "--source=functions/extract-rss/main.py", "--target=extract_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy extract-service --image gcr.io/ba882-rgk/my-container --platform managed

# Dockerfile for transform
# CMD ["functions-framework", "--source=functions/parse-rss/main.py", "--target=transform_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy transform-service --image gcr.io/ba882-rgk/my-container --platform managed

# Dockerfile for load
# CMD ["functions-framework", "--source=functions/load-rss/main.py", "--target=load_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy load-service --image gcr.io/ba882-rgk/my-container --platform managed