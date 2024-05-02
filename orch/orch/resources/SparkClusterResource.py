from dagster import ConfigurableResource
from dagster_gcp import DataprocResource


class SparkClusterResource(ConfigurableResource):
    cluster_config: dict

    def submit_job(self, job_config: dict) -> None:
        """Submits a Spark job and waits to the cluster using the provided configuration."""
        dataproc = DataprocResource(
            project_id=self.cluster_config["projectId"],
            region=self.cluster_config["region"],
            cluster_name=self.cluster_config["clusterName"],
            cluster_config_dict=self.cluster_config["cluster_config"],
        )

        client = dataproc.get_client()
        client.create_cluster()
        try:
            job = client.submit_job(job_config)
            client.wait_for_job(job_id=job["reference"]["jobId"])
        except Exception as e:
            raise e
        finally:
            client.delete_cluster()


if __name__ == "__main__":
    from ..config.dataproc_config import dataproc_create_cluster_config, dataproc_pyspark_job_config
    sp = SparkClusterResource(cluster_config=dataproc_create_cluster_config)
    sp.submit_job(dataproc_pyspark_job_config)