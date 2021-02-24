---
layout: default
---
<!-- TOC depthFrom:2 depthTo:6 orderedList:false updateOnSave:true withLinks:true -->

- [Deployment Guide](#deployment-guide)
  - [Before you begin](#before-you-begin)
  - [Setting up backend services on Google Cloud](#setting-up-backend-services-on-google-cloud)
    - [Starting the cache store](#starting-the-cache-store)
    - [Set up Serverless VPC Access](#set-up-serverless-vpc-access)
    - [Cloud Firestore in Datastore mode](#cloud-firestore-in-datastore-mode)
    - [Cloud Storage](#cloud-storage)
    - [Deploying](#deploying)
  - [Check to see everything worked](#check-to-see-everything-worked)
  - [Next steps](#next-steps)

<!-- /TOC -->

# Deployment Guide

This page explains how to quickly deploy an Open Saves server to Cloud Run on MacOS/Linux.

## Before you begin

You may want to create a new project for this quickstart, as we create a Datastore instance
and it can only be done once per project. This also allows you to easily delete
the project after you are done with it.

To build and deploy the Open Saves servers, you need to
install and configure the following:

1. Download and install the [Google Cloud SDK](https://cloud.google.com/sdk/install).

    For more information on installation and set up, see the
    [Cloud SDK Quickstarts](https://cloud.google.com/sdk/docs/quickstarts).

1. Create a new Google Cloud project using the [Google Cloud Console](https://console.cloud.google.com/) or the Google Cloud SDK. See [Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects) for additional information.

## Setting up backend services on Google Cloud

You need to set up Memorystore, Cloud Firestore in [Datastore mode (Datastore)](https://cloud.google.com/datastore/docs/firestore-or-datastore), and
Cloud Storage to run the current version of Open Saves.

Memorystore is used for caching records for faster lookups.
Cloud Firestore in Datastore mode (Datastore) is primarily used to manage
metadata of Open Saves. Smaller blob data (usually up to a few kilobytes) could
also be stored in Datastore.
Lastly, Cloud Storage is used to store all large blob data that cannot fit
into Datastore. By default, blobs larger than 64 KiB are stored in Cloud Storage.

First, start by exporting the following environment variables:

```bash
export GCP_PROJECT=$(gcloud config get-value project)
export GCP_REGION=us-central1
export REDIS_ID=open-saves-redis
export REDIS_PORT="6379"
export VPC_CONNECTOR=open-saves-vpc
```

### Starting the cache store

Run the following command to create a Redis instance by using [Memorystore](https://cloud.google.com/memorystore). This
will give you an instance with a private IP, which you pass into Open Saves.

```bash
gcloud redis instances create --region=$GCP_REGION $REDIS_ID
```

This may take a while. After this has been created, run the following command to find
the private IP address of this instance:

```bash
gcloud redis instances describe --region=$GCP_REGION $REDIS_ID | grep "host:"
```

Then, save this private IP address to another environment variable:

```bash
export REDIS_IP=<your ip here>
```

### Set up Serverless VPC Access

By default, because our Redis instance in Memorystore only has a private IP, we need to create a VPC connector
so that Cloud Run can talk to Memorystore properly.

We will be loosely following [these instructions](https://cloud.google.com/memorystore/docs/redis/connect-redis-instance-cloud-run) to
set up a VPC connector.

First, export your network name.

```bash
export VPC_NETWORK="projects/$GCP_PROJECT/global/networks/default"
```

Ensure the previous value matches the output of the following command.

```bash
gcloud redis instances describe $REDIS_ID --region $GCP_REGION --format "value(authorizedNetwork)"
```

Next, enable the Serverless VPC Access API for your project.

```bash
gcloud services enable vpcaccess.googleapis.com
```

Create the VPC connector. This step may take some time.

```bash
gcloud compute networks vpc-access connectors create $VPC_CONNECTOR \
--network $VPC_NETWORK \
--region $GCP_REGION \
--range 10.8.0.0/28
```

Verify that your connector is in the READY state before using it:

```bash
gcloud compute networks vpc-access connectors describe $VPC_CONNECTOR --region $GCP_REGION
```

The output should contain the line state: READY.

### Cloud Firestore in Datastore mode

Follow the [quickstart guide](https://cloud.google.com/datastore/docs/quickstart)
to set up a database in **Datastore** mode.

![datastore_mode](images/datastore.png)

You may choose whichever region you like, however, it can only be specified once
and cannot be undone. Google Cloud Platform currently allows only one Datastore
database per project, so you would need to create a new project to change the
database location.

### Cloud Storage

Cloud Storage is used to store large blobs that don't fit in Datastore.

Create a simple bucket to hold all open saves blobs. This bucket has to be globally
unique.

```bash
export BUCKET_PATH=gs://<your-unique-bucket>
gsutil mb $BUCKET_PATH
```

### Deploying

Run the following commands to deploy the containerized application to Cloud Run:
This uses the beta version of the Cloud Run service because we are using the
VPC connector feature.

```bash
export TAG=gcr.io/triton-for-games-dev/triton-server:testing
export SERVICE_NAME="open-saves"
gcloud beta run deploy $SERVICE_NAME \
                  --platform=managed \
                  --region=$GCP_REGION \
                  --image=$TAG \
                  --set-env-vars="OPEN_SAVES_BUCKET="$BUCKET_PATH \
                  --set-env-vars="OPEN_SAVES_PROJECT="$GCP_PROJECT \
                  --set-env-vars="OPEN_SAVES_CACHE"=$REDIS_IP":"$REDIS_PORT \
                  --allow-unauthenticated \
                  --vpc-connector $VPC_CONNECTOR
```

Grab the endpoint and save it to an environment variable. 

```bash
ENDPOINT=$(\
gcloud run services list \
  --project=$GCP_PROJECT \
  --region=$GCP_REGION \
  --platform=managed \
  --format="value(status.address.url)" \
  --filter="metadata.name="$SERVICE_NAME)

ENDPOINT=${ENDPOINT#https://} && echo ${ENDPOINT}
```

Finally, clone this repository and try the example code
to make sure it works. You will need Go1.15 or later for this to work.

```bash
git clone https://github.com/googleforgames/open-saves.git
go run examples/grpc-client/main.go -address=$ENDPOINT:443
```

This sample code creates a new store, a new record with inline properties,
fetches that record, and creates a new record with a large blob attached.

## Check to see everything worked

Navigate to the [Datastore Dashboard](https://console.cloud.google.com/datastore).
You should see the entities that you created with properties like "RecordKey",
"Size", "Status", "StoreKey", and "Timestamps". Try filtering by "record",
"blob", and "store" in the "Kind" search bar.

Next, navigate to [Memorystore](https://console.cloud.google.com/memorystore/redis/instances).
Select your instance, and under the graph type, select "Keys in database".
You should see keys in this part, indicating that some of the records have been cached properly.

Lastly, use the `gsutil` to list items in your bucket. You should see one large item there.

```bash
gsutil ls $BUCKET_NAME
```

## Next steps

The basic Open Saves server **does not have authentication / authorization**. We recommend following this guide on [Authenticating service-to-service](https://cloud.google.com/run/docs/authenticating/service-to-service) to add proper authentication before deploying to production.
