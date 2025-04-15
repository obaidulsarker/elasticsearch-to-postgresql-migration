#!/bin/bash

NAMESPACE_NAME=obaidul
AUTOMATION_APP_VERSION=1.0
AUTOMATION_APP_REPO=elasticsearch-to-postgresql

echo "Building $AUTOMATION_APP_REPO"
sudo docker build -f Dockerfile . -t $NAMESPACE_NAME/$AUTOMATION_APP_REPO:$AUTOMATION_APP_VERSION
sudo docker build -f Dockerfile . -t $NAMESPACE_NAME/$AUTOMATION_APP_REPO:latest

echo "Pushing $AUTOMATION_APP_REPO"
sudo docker push $NAMESPACE_NAME/$AUTOMATION_APP_REPO:$AUTOMATION_APP_VERSION
sudo docker push $NAMESPACE_NAME/$AUTOMATION_APP_REPO:latest


