# set the project
gcloud config set project ba882-rgk


echo "======================================================"
echo "deploying the post length training function"
echo "======================================================"

gcloud functions deploy ml-postwc-train \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./ml/functions/ml-post-wc-train \
    --stage-bucket rgk-ba882-fall24-functions \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 60s 

echo "======================================================"
echo "deploying the post length inference function"
echo "======================================================"

gcloud functions deploy ml-postwc-serve \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./ml/functions/ml-post-wc-serve \
    --stage-bucket rgk-ba882-fall24-functions \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 60s 