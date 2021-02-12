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
  - [Configuring the server](#configuring-the-server)

<!-- /TOC -->

# Deployment Guide

This page explains how to quickly deploy an Open Saves server.

## Before you begin

To build and deploy the Open Saves servers, you need to
install and configure the following:

1. Download and install the [Google Cloud SDK](https://cloud.google.com/sdk/install).

    For more information on installation and to set up, see the
    [Cloud SDK Quickstarts](https://cloud.google.com/sdk/docs/quickstarts).

## Setting up backend services on Google Cloud

You need to set up Memorystore, Cloud Firestore in Datastore mode (Datastore), and
Cloud Storage to run the current version of Open Saves.

First, start by exporting a few environment variables

```bash
export GCP_PROJECT=<your-project-here>
export GCP_REGION=us-central1
export REDIS_NAME=open-saves-redis
export BUCKET_PATH=gs://<your-new-bucket>
```

### Starting the cache store

Create a Redis instance by using [Memorystore](https://cloud.google.com/memorystore). This
will give you an instance with a private IP, which you will need to pass in to Triton.

```bash
gcloud redis instances create --region=$GCP_REGION $REDIS_NAME
```

This may take a while. After this has been created, run the following commands to find
the private IP address of this instance.

```bash
gcloud redis instances describe --region=$GCP_REGION $REDIS_NAME | grep "host:"
```

Then, save this to another environment variable.

```bash
export REDIS_IP=<your ip here>:6379
```

### Set up Serverless VPC Access

Follow [these instructions](https://cloud.google.com/memorystore/docs/redis/connect-redis-instance-cloud-run) to set up a VPC connector. Then, export the name of the VPC connector as an environment variable.

```bash
export CONNECTOR_NAME=<your-connnector-name>
```

### Cloud Firestore in Datastore mode

Cloud Firestore in Datastore mode (Datastore) is primarily used to manage
metadata of Open Saves. Smaller blob data (usually up to a few kilobytes) could
also be stored in Datastore.

Please follow the [quick start guide](https://cloud.google.com/datastore/docs/quickstart)
to set up a database in Datastore mode. You may choose whichever region you
like, however, it can only be specified one and cannot be undone. Google Cloud
Platform currently allows only one Datastore database per project, so you
would need to create a new project to change the database location.

### Cloud Storage

Cloud Storage is used to store large blobs that don't fit in Datastore.

Let's create a simple bucket to hold our resources.

```bash
gsutil mb $BUCKET_PATH
```

### Deploying

```bash
export TAG=gcr.io/triton-for-games-dev/triton-server:testing
export SERVICE_NAME="open-saves"
gcloud beta run deploy $SERVICE_NAME \
                  --platform=managed \
                  --region=$GCP_REGION \
                  --image=$TAG \
                  --set-env-vars="OPEN_SAVES_BUCKET="$BUCKET_PATH \
                  --set-env-vars="OPEN_SAVES_PROJECT="$GCP_PROJECT \
                  --set-env-vars="OPEN_SAVES_CACHE"=$REDIS_IP \
                  --allow-unauthenticated \
                  --vpc-connector $CONNECTOR_NAME
```

Grab the endpoint and try the example code to make sure it works

```bash
ENDPOINT=$(\
gcloud run services list \
  --project=$GCP_PROJECT \
  --region=$GCP_REGION \
  --platform=managed \
  --format="value(status.address.url)" \
  --filter="metadata.name="$SERVICE_NAME)

ENDPOINT=${ENDPOINT#https://} && echo ${ENDPOINT}

go run examples/grpc-all/main.go -address=$ENDPOINT:443
```


## Configuring the server

The basic Open Saves server **does not have authentication / authorization**. We recommend following this guide on [Authenticating service-to-service](https://cloud.google.com/run/docs/authenticating/service-to-service) to add proper authenticationn before deploying to production.
