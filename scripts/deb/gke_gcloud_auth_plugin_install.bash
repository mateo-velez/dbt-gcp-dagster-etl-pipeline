#!/usr/bin/env bash

set -o nounset   # Treat unset variables as an error
set -o errexit   # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Fail if any command in a pipeline fails

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update && sudo apt-get install google-cloud-sdk-gke-gcloud-auth-plugin


