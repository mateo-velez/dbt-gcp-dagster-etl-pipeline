import datetime
from .constants import PROJECT_ID, REGION_ID, MISC_BUCKET_NAME
# General config
#Edit this file
general_config = {
    "BUCKET": MISC_BUCKET_NAME,
    "PROJECT_ID":PROJECT_ID,
}

# dataproc config
dataproc_config = {
    "REGION": REGION_ID,
    "CLUSTER_NAME": "spark-cluster",
    "INIT_ACTIONS": "<gcs path>",
    "IMAGE": "2.1-debian11"
}

dataproc_create_cluster_config = {
  "projectId": general_config["PROJECT_ID"],
  "region": dataproc_config["REGION"],
  "clusterName": dataproc_config["CLUSTER_NAME"],
  "cluster_config": {
    "configBucket": "",
    "gceClusterConfig": {
      "networkUri": "",
      "subnetworkUri": "",
      "internalIpOnly": False,
      "zoneUri": "",
      "metadata": {},
      "tags": [],
      "shieldedInstanceConfig": {
        "enableSecureBoot": False,
        "enableVtpm": False,
        "enableIntegrityMonitoring": False
      }
    },
    "masterConfig": {
      "numInstances": 1,
      "machineTypeUri": "n2-standard-4",
      "diskConfig": {
        "bootDiskType": "pd-standard",
        "bootDiskSizeGb": 500,
        "numLocalSsds": 0,
        "localSsdInterface": "SCSI"
      },
      "minCpuPlatform": "",
      "imageUri": ""
    },
    "softwareConfig": {
      "imageVersion": dataproc_config["IMAGE"],
      "properties": {
        "dataproc:dataproc.allow.zero.workers": "true"
      }
    },
    "lifecycleConfig": {
      "idleDeleteTtl": "1800s"
    }
}
}

# pySPARK config
pyspark_config = {
    "PYTHON_FILE" : "gs://"+general_config["BUCKET"]+"/spark/albion_dump_market_data.py",
}

dataproc_pyspark_job_config = {
    "projectId": general_config["PROJECT_ID"],
    "job": {
        "placement": {"clusterName": dataproc_config["CLUSTER_NAME"]},
        "reference": {
            "jobId": "stack-pyspark-job" + datetime.datetime.now().strftime("%m%d%Y%H%M%S"),
            "projectId": general_config["PROJECT_ID"]
        },
        "pysparkJob": {
            "mainPythonFileUri": pyspark_config["PYTHON_FILE"],
            "jarFileUris": [
            ]
        }
    }
}

