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

# gcloud functions deploy dev-schema-setup \
#     --gen2 \
#     --runtime python311 \
#     --trigger-http \
#     --entry-point task \
#     --source . \
#     --stage-bucket ba882-rgk-bucket1 \
#     --service-account service1@ba882-rgk.iam.gserviceaccount.com \
#     --region us-central1 \
#     --allow-unauthenticated \
#     --memory 512MB 

gcloud functions deploy dev-schema-setup \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task
    --source . \
    --stage-bucket ba882-rgk-bucket1 \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB

# daily setup
echo "======================================================"
echo "deploying the daily setup"
echo "======================================================"

# gcloud functions deploy daily_update \
#     --gen2 \
#     --runtime python311 \
#     --trigger-http \
#     --entry-point daily_update \  
#     --source ./882-cloudshell-fall24 \
#     --stage-bucket ba882-rgk-bucket1 \
#     --service-account service1@ba882-rgk.iam.gserviceaccount.com \
#     --region us-central1 \
#     --allow-unauthenticated \
#     --memory 512MB 

# gcloud functions deploy daily_update \
#     --gen2 \
#     --runtime python311 \
#     --trigger-http \
#     --entry-point daily_update \  
#     --source ./functions/schema-setup \
#     --stage-bucket ba882-rgk-bucket1 \
#     --service-account service1@ba882-rgk.iam.gserviceaccount.com \
#     --region us-central1 \
#     --allow-unauthenticated \
#     --memory 512MB 