FROM python:3.11-slim as base

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagster-webserver and dagster-daemon, and to load the DagsterInstance

# Add repository code

COPY . /opt/dagster/app/orch
WORKDIR /opt/dagster/app/orch

RUN pip install --upgrade pip

RUN pip install . \
    dagster \
    dagster-postgres \
    dagster-docker


ENV PROJECT_ID="_"
ENV ENVIRONMENT="_"
ENV REGION="_"
ENV RAW_BUCKET_NAME="_"
ENV STAGING_BUCKET_NAME="_"
ENV MISC_BUCKET_NAME="_"
ENV DBT_DATASET_ID="_"
ENV GOOGLE_APPLICATION_CREDENTIALS="_"


RUN cd dbt_assets && dbt parse


# Run dagster gRPC server on port 4000
EXPOSE 3030


WORKDIR /opt/dagster/app/orch


FROM base as test 
RUN pip install .[test]

CMD [ "pytest", "orch_tests/" ]


FROM base as prod

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3030", "-m", "orch"]