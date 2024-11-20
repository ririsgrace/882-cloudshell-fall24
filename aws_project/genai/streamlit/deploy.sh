
# setup the project
gcloud config set project ba882-rgk

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/ba882-rgk/streamlit-genai-apps .

echo "======================================================"
echo "push"
echo "======================================================"

docker push gcr.io/ba882-rgk/streamlit-genai-apps

echo "======================================================"
echo "deploy run"
echo "======================================================"


gcloud run deploy streamlit-genai-apps \
    --image gcr.io/ba882-rgk/streamlit-genai-apps \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --service-account service1@ba882-rgk.iam.gserviceaccount.com \
    --memory 1Gi
