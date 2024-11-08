# from prefect import flow

# if __name__ == "__main__":
#     flow.from_source(
#         source="https://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@github.com/ririsgrace/882-cloudshell-fall24.git",  # Adjust this with your GitHub repo URL
#         entrypoint="flow/etl.py:etl_flow",  # Adjust this to your actual entrypoint for the etl_flow function
#     ).deploy(
#         name="daily-stock-etl",
#         work_pool_name="rgk-pool1",  # Replace with your work pool name
#         job_variables={"env": {}, "pip_packages": ["pandas", "requests"]},
#         # cron="0 18 * * *",  
#         cron="0 22 * * *", # This schedules the flow to run daily at 6 PM
#         tags=["prod"],
#         description="ETL pipeline to download stock data daily from Yahoo Finance and process it.",
#         version="1.0.0",
#     )
from prefect import flow
import os

# Set Prefect API key and workspace
PREFECT_API_KEY = os.getenv('PREFECT_API_KEY')
# PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/[YOUR-ACCOUNT-ID]/workspaces/[YOUR-WORKSPACE-ID]"
PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/fbd3de01-88a1-49f3-be1e-3fc60e32454d/workspaces/55df536c-cf2b-41eb-860b-d2d43d0b63c8"
 

if __name__ == "__main__":
    try:
        # Configure Prefect Cloud connection
        os.environ["PREFECT_API_URL"] = PREFECT_API_URL
        
        # Login to Prefect Cloud
        os.system(f'prefect cloud login --key {PREFECT_API_KEY} --workspace "Riris Grace Karolina/55df536c-cf2b-41eb-860b-d2d43d0b63c8"')
        
        # Create work pool if it doesn't exist
        os.system('prefect work-pool create rgk-pool1 --type process')
        
        # Deploy the flow
        flow.from_source(
            source="https://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@github.com/ririsgrace/882-cloudshell-fall24.git",
            entrypoint="flow/etl.py:etl_flow",
        ).deploy(
            name="daily-stock-etl",
            work_pool_name="rgk-pool1",
            job_variables={
                "env": {},
                "pip_packages": ["pandas", "requests"]
            },
            cron="0 18 * * *",
            tags=["prod"],
            description="ETL pipeline to download stock data daily from Yahoo Finance and process it.",
            version="1.0.0",
        )
    except Exception as e:
        print(f"Error during deployment: {str(e)}")
        raise e