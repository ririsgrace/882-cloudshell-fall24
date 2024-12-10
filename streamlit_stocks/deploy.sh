# setup the project
gcloud config set project ba882-rgk

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/ba882-rgk/streamlit-stocks .

echo "======================================================"
echo "push"
echo "======================================================"

docker push gcr.io/ba882-rgk/streamlit-stocks

echo "======================================================"
echo "deploy run"
echo "======================================================"


gcloud run deploy streamlit-stocks \
    --image gcr.io/ba882-rgk/streamlit-stocks \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --memory 1Gi