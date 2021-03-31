# Deploying to Google Kubernetes Engine (GKE)

## Prerequisites

Follow the steps in the [Deployment Guide](deploying.md) before running the commands on this page.

## Steps

This step requires `kubectl` to be installed.

Similar to Cloud Run, GKE depends on an image that we can build and push to GCR.

```bash
export GCP_PROJECT="your-project-id"
export TAG=gcr.io/$GCP_PROJECT/open-saves-server:testing
docker build -t $TAG .
docker push $TAG
```

Next, we need to create a cluster that has Workload Identity enabled. It may take several minutes to create a cluster.
For more information on Workload Identity, see [here](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity).

```bash
export GCP_REGION="us-west1"
gcloud config set compute/region $GCP_REGION

gcloud container clusters create open-saves-cluster \
  --workload-pool=$GCP_PROJECT.svc.id.goog
```

Next, add a new node pool to the cluster with Workload Identity enabled. This step also takes a few minutes to complete.

```bash
gcloud container node-pools create open-saves-nodepool \
  --cluster=open-saves-cluster \
  --workload-metadata=GKE_METADATA
```

Configure `kubectl` to communicate with the cluster.

```bash
gcloud container clusters get-credentials open-saves-cluster
```

Create a separate namespace for the Kubernetes service account.

```bash
export OPEN_SAVES_NAMESPACE="open-saves-namespace"
kubectl create namespace $OPEN_SAVES_NAMESPACE
```

Create the Kubernetes service account to use for your application.

```bash
export OPEN_SAVES_KSA="open-saves-ksa"
kubectl create serviceaccount --namespace $OPEN_SAVES_NAMESPACE $OPEN_SAVES_KSA
```

Create a new service account for your application.

```bash
export OPEN_SAVES_GSA="open-saves-gsa"
gcloud iam service-accounts create $OPEN_SAVES_GSA
```

Next, you will need to grant this service account access to the services
needed for Open Saves to function, including Memorystore (Redis), Datastore, and Storage.

```bash
gcloud projects add-iam-policy-binding $GCP_PROJECT \
  --member="serviceAccount:$OPEN_SAVES_GSA@$GCP_PROJECT.iam.gserviceaccount.com" \
  --role="roles/redis.editor"

gcloud projects add-iam-policy-binding $GCP_PROJECT \
  --member="serviceAccount:$OPEN_SAVES_GSA@$GCP_PROJECT.iam.gserviceaccount.com" \
  --role="roles/datastore.user"

gcloud projects add-iam-policy-binding $GCP_PROJECT \
  --member="serviceAccount:$OPEN_SAVES_GSA@$GCP_PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

Add the IAM policy binding for your service account to use the workload identity.

```bash
gcloud iam service-accounts add-iam-policy-binding \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:$GCP_PROJECT.svc.id.goog[$OPEN_SACES_NAMESPACE/$OPEN_SAVES_KSA]" \
  $OPEN_SAVES_GSA@$GCP_PROJECT.iam.gserviceaccount.com
```

Annotate the Kubernetes service account with your Google service account.

```bash
kubectl annotate serviceaccount \
  --namespace=$OPEN_SAVES_NAMESPACE \
  $OPEN_SAVES_KSA \
  iam.gke.io/gcp-service-account=$OPEN_SAVES_GSA@$GCP_PROJECT.iam.gserviceaccount.com
```

Deploy to GKE.

```bash
cd deploy/gke
kubectl apply -f deployment.yaml
kubectl get deployment -n $OPEN_SAVES_NAMESPACE
```

Deploy the load balancer service and wait for the external IP to become available

```bash
kubectl apply -f service.yaml
kubectl get services -n $OPEN_SAVES_NAMESPACE
```

Using the endpoint from the previous step, try running the example client to make sure everything was set up properly.

```bash
cd ../..
export ENDPOINT="12.34.56.78:6000" # your endpoint from previous step
go run examples/grpc-client/main.go -address=$ENDPOINT -insecure=true
```

Alternatively, if you have `grpc_cli` installed, test you can see the functions using:

```bash
$ grpc_cli ls $ENDPOINT opensaves.OpenSaves
CreateStore
GetStore
ListStores
DeleteStore
CreateRecord
GetRecord
QueryRecords
UpdateRecord
DeleteRecord
Ping
```
