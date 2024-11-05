from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/ririsgrace/882-cloudshell-fall24.git",
        entrypoint="aws_project/prefect/flows/ml-views.py:ml_datasets",
    ).deploy(
        name="ml-datasets",
        work_pool_name="rgk-pool1",
        job_variables={"env": {"RGK": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        cron="20 0 * * *",
        tags=["prod"],
        description="The pipeline to create ML datasets off of the staged data.  Version is just for illustration",
        version="1.0.0",
    )
