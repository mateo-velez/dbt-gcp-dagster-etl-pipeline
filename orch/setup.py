from setuptools import find_packages, setup

setup(
    name="orch",
    packages=find_packages(exclude=["orch_tests"]),
    install_requires=[
        "dagster",
        "nltk",
        "pandas",
        "google-cloud-storage",
        "google-cloud-dataproc",
        "google-cloud-bigquery",
        "dagster-gcp",
        "dagster-dbt",
        "dbt-bigquery",
    ],
    extras_require={"dev": ["dagster-webserver"], 
                    "test": ["pytest", "faker"]},
)
