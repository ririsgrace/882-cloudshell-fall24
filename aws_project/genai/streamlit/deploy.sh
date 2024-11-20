
# setup the project
gcloud config set project ba882-labs

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/ba882-labs/streamlit-genai-apps .

echo "======================================================"
echo "push"
echo "======================================================"

docker push gcr.io/ba882-labs/streamlit-genai-apps

echo "======================================================"
echo "deploy run"
echo "======================================================"


gcloud run deploy streamlit-genai-apps \
    --image gcr.io/ba882-labs/streamlit-genai-apps \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --service-account labs-account@ba882-labs.iam.gserviceaccount.com \
    --memory 1Gi