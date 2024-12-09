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
    && pip install functions-framework \
    && pip install prefect

# Ensure the environment is activated at runtime (fixed format)
ENV PATH="/opt/conda/envs/new-env/bin:$PATH"

# Copy the rest of the application to the container
COPY . /app

# Expose port 8080
EXPOSE 8080

# Dockerfile for schema-setup
# CMD ["functions-framework", "--source=functions/schema-setup/main.py", "--target=schema_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy schema-service --image gcr.io/ba882-rgk/my-container --platform managed

# Dockerfile for extract
CMD ["functions-framework", "--source=functions/extract-rss/main.py", "--target=extract_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy extract-service --image gcr.io/ba882-rgk/my-container --platform managed

# Dockerfile for transform
# CMD ["functions-framework", "--source=functions/parse-rss/main.py", "--target=transform_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy transform-service --image gcr.io/ba882-rgk/my-container --platform managed

# Dockerfile for load
# CMD ["functions-framework", "--source=functions/load-rss/main.py", "--target=load_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy load-service --image gcr.io/ba882-rgk/my-container --platform managed

# Dockerfile for model-1
# CMD ["functions-framework", "--source=functions/ml/model-1/main.py", "--target=model1_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy model1-service --image gcr.io/ba882-rgk/my-container --platform managed

# Dockerfile for hyper parameter tuning
# CMD ["functions-framework", "--source=functions/ml/hyper/main.py", "--target=hyper_task", "--port=8080"]
# to deploy, use this command: gcloud run deploy hyper-service --image gcr.io/ba882-rgk/my-container --platform managed 


# Dockerfile for prefect flow daily update
# Run the Prefect flow deployment
# CMD ["python", "flow/deploy_flow.py"]
# to run the job, use this command *replace the [YOUR-ACCOUNT-ID] and [YOUR-WORKSPACE-ID]: 
# gcloud beta run jobs create daily-etl-job \
#   --image gcr.io/ba882-rgk/my-container \
#   --region us-central1 \
#   --set-secrets="PREFECT_API_KEY=prefect-api-key:latest" \
#   --set-env-vars="PREFECT_API_URL=https://api.prefect.cloud/api/accounts/[YOUR-ACCOUNT-ID]/workspaces/[YOUR-WORKSPACE-ID]" \
#   --memory 1Gi \
#   --command python \
#   --args "flow/deploy_flow.py"

# to update the jobs deployed (already exist) and you want to update, use this command: 
# gcloud beta run jobs update daily-etl-job \
#   --image gcr.io/ba882-rgk/my-container \
#   --region us-central1 \
#   --set-secrets="PREFECT_API_KEY=prefect-api-key:latest" \
#   --set-env-vars="PREFECT_API_URL=https://api.prefect.cloud/api/accounts/fbd3de01-88a1-49f3-be1e-3fc60e32454d/workspaces/55df536c-cf2b-41eb-860b-d2d43d0b63c8" \
#   --memory 1Gi \
#   --command python \
#   --args "flow/deploy_flow.py"

# PREFECT
# fbd3de01-88a1-49f3-be1e-3fc60e32454d = account id
# 55df536c-cf2b-41eb-860b-d2d43d0b63c8 = workspace id
# set the link for prefect on terminal: prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/fbd3de01-88a1-49f3-be1e-3fc60e32454d/workspaces/55df536c-cf2b-41eb-860b-d2d43d0b63c8"
# API key: pnu_JbhH0OYfUZkeLnnYpdXXll5YD95CK22kyb66
# command: prefect cloud login -k pnu_JbhH0OYfUZkeLnnYpdXXll5YD95CK22kyb66
# or: export PREFECT_API_KEY="pnu_JbhH0OYfUZkeLnnYpdXXll5YD95CK22kyb66"
# check the work pool: prefect work-pool ls

# to trigger manually the scheduler: 
# 1. First, run the Cloud Run job (if you haven't already or if you made changes):
# command: gcloud beta run jobs execute daily-etl-job
# 2. Then, run the actual ETL:
# command: prefect deployment run 'stock-etl-flow/daily-stock-etl'
# 3. check the status in the Prefect UI using the URL:
# link: https://app.prefect.cloud/account/fbd3de01-88a1-49f3-be1e-3fc60e32454d/workspace/55df536c-cf2b-41eb-860b-d2d43d0b63c8/runs/flow-run/7ae8ec35-10c8-4962-bc25-e537381084b0

# Pinecone API token: pcsk_6TbH2T_DidcVfsAYy1cuHJg3Tnjh8HzKiuAkWRqFVEWjRk4mNXJ2XVezGdAgKjNhJ3MTvY