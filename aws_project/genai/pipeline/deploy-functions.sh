######
## simple script for now to deploy functions
## deploys all, which may not be necessary for unchanged resources
######

# setup the project
gcloud config set project ba882-rgk

# schema setup
echo "======================================================"
echo "deploying the schema setup"
echo "======================================================"

gcloud functions deploy genai-schema-setup \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/schema-setup \
    --stage-bucket rgk-ba882-fall24-awsblogs \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB 

# to identify posts that haven't been ingested
echo "======================================================"
echo "deploying the record collector"
echo "======================================================"

gcloud functions deploy genai-schema-collector \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/collector \
    --stage-bucket rgk-ba882-fall24-awsblogs \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB 

# a function that will parse a single post passed to it
echo "======================================================"
echo "deploying the record ingestor"
echo "======================================================"

gcloud functions deploy genai-schema-ingestor \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/ingestor \
    --stage-bucket rgk-ba882-fall24-awsblogs \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB
