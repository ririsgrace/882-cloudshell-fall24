import streamlit as st
import pandas as pd
import vertexai
from vertexai.generative_models import GenerativeModel, ChatSession
from google.cloud import secretmanager
import duckdb
import yfinance as yf

# Page configuration
st.set_page_config(
    page_title="Stocks Dashboard",
    page_icon=":robot:",
    layout="wide",
)

# Custom CSS to change the background to blue color
st.markdown("""
    <style>
        body {
            background-color: #2e8bc0; 
            color: white;
        }
        .block-container {
            background-color: transparent;
        }
        h1, h2, h3, h4, h5, h6 {
            color: white;
        }
        .css-1v3fvcr {
            background-color: #2e8bc0 !important;
        }
    </style>
    """, unsafe_allow_html=True)

# Display the image from GCS
st.image(
    "https://storage.cloud.google.com/rgk-ba882-fall24-finance/streamlit/yfinance.png",  # Image URL from GCS
    caption="Stocks Dashboard",
    use_container_width=False,
    width=400,  # Adjust image size as needed
)

# Initialize Vertex AI
GCP_PROJECT = 'ba882-rgk'
GCP_REGION = "us-central1"
vertexai.init(project=GCP_PROJECT, location=GCP_REGION)

model = GenerativeModel("gemini-1.5-flash-002")
chat_session = model.start_chat(response_validation=False)

# Setup
project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'
db = 'stocks_10'
schema = 'stocks_schema_10'
db_schema = f"{db}.{schema}"

# Secret Manager connection
sm = secretmanager.SecretManagerServiceClient()
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")

# MotherDuck connection
md = duckdb.connect(f'md:?motherduck_token={md_token}')

# Function to fetch stock data
def fetch_data():
    query = """
    SELECT Date, Ticker, Open, Close, Volume
    FROM stocks_10.stocks_schema_10.combined_stocks
    ORDER BY Date DESC, Ticker
    LIMIT 20
    """
    data = md.execute(query).fetchdf()  # Fetch result as a Pandas DataFrame

    return data


# Function to fetch ML predictions
def fetch_ml_predictions():
    query = """
    SELECT Date, Ticker, Actual, Predicted, roe, rmse, mape, prediction_timestamp
    FROM `stocks.stocks_schema.predicted_stock`
    ORDER BY prediction_timestamp DESC, Date DESC
    LIMIT 10
    """
    predictions = md.execute(query).fetchdf()  # Fetch result as a Pandas DataFrame

    return predictions

# Function to get chat response from Vertex AI
def get_chat_response(chat: ChatSession, prompt: str, dataset: pd.DataFrame) -> str:
    # Convert the first 50 rows of the DataFrame to a JSON-like format (records)
    dataset_sample = dataset.head(20).to_dict(orient='records')  # Get the first 50 rows

    # Summarize the dataset context (optionally provide aggregation if necessary to reduce size)
    dataset_summary = f"""
    These are the first 20 rows of your stock data:
    {dataset_sample}
    You can ask me questions about trends, stock volume, stock returns, or any specific ticker.
    """

    # Full prompt with the dataset sample
    full_prompt = f"{dataset_summary}\n\nQuestion: {prompt}"

    # Get the chatbot's response
    text_response = []
    responses = chat.send_message(full_prompt, stream=True)
    for chunk in responses:
        text_response.append(chunk.text)
    
    return "".join(text_response)

# App UI
st.markdown("### Welcome! Hope you like our YFinance Stocks Dashboard!")

# Fetch and display stock data
try:
    stocks_df = fetch_data() # Fetch stock data
    ml_predictions_df = fetch_ml_predictions()  # Fetch ML predictions

    # Create tabs for Stocks Data, YFinbot, and ML Performance
    tab1, tab2, tab3 = st.tabs(["💰 Stocks Data", "🤖 YFinbot", " 🧠 ML Performance"])
    
    # Tab 1: Stocks Data
    with tab1:
        st.subheader("Recent Stock Price Data for 10 Tech Companies")
        st.dataframe(stocks_df)  # Display stock data in a table

        # Summary Statistics
        st.subheader("Summary Statistics")
        stats = {
            "Average Stock Volume": stocks_df['Volume'].mean(),
            "Highest Open Price": stocks_df['Open'].max(),
            "Median Close Price": stocks_df['Close'].median(),
        }
        for key, value in stats.items():
            st.write(f"- **{key}:** {value:.2f}")

    # Tab 2: YFinbot
    with tab2:
        st.subheader("Ask the Chatbot")
        prompt = st.text_input("Ask a question about your stocks data:")
        if prompt:
            response = get_chat_response(chat_session, prompt, stocks_df)
            st.markdown(f"**Chatbot Response:** {response}")
        
        # Add note below the chatbot informing users
        st.markdown("**Note**: The chatbot can only answer questions based on the last 2 weekdays.")

    # Tab 3: ML Performance
    with tab3:
        st.subheader("ML Prediction Performance for last 10 days")
        st.dataframe(ml_predictions_df)  # Display ML predictions in a table

except Exception as e:
    st.error(f"An error occurred: {e}")

# Footer
st.markdown("---")
st.markdown("Team 1: Chris Kuo, Xinyuan Hu, Setu Shah, Riris Grace Karolina, Jenn Hong")
