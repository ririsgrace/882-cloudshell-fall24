from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/ririsgrace/882-cloudshell-fall24.git",  # Adjust this with your GitHub repo URL
        entrypoint="flow/etl.py:etl_flow",  # Adjust this to your actual entrypoint for the etl_flow function
    ).deploy(
        name="daily-stock-etl",
        work_pool_name="rgk-pool1",  # Replace with your work pool name
        job_variables={"env": {}, "pip_packages": ["pandas", "requests"]},
        #cron="0 18 * * *",  
        cron="0 22 * * *", # This schedules the flow to run daily at 6 PM
        tags=["prod"],
        description="ETL pipeline to download stock data daily from Yahoo Finance and process it.",
        version="1.0.0",
    )