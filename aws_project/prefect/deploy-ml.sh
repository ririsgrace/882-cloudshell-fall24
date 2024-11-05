# set the project
gcloud config set project ba882-rgk


echo "======================================================"
echo "deploying the ml dataset: post-tags"
echo "======================================================"

gcloud functions deploy ml-post-tags \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/post-tags \
    --stage-bucket rgk-ba882-fall24-functions \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB  \
    --timeout 300s 

# load the feeds into raw and changes into stage
echo "======================================================"
echo "deploying the ml dataset: post-length"
echo "======================================================"

gcloud functions deploy ml-post-length \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/post-wc \
    --stage-bucket rgk-ba882-fall24-functions \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB  \
    --timeout 300s 