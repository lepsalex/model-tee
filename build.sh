#!/bin/bash

VERSION="4.2.0-SNAPSHOT"

docker build . -t lepsalex/model-tee:$VERSION -t lepsalex/model-tee:latest
docker push lepsalex/model-tee:$VERSION
docker push lepsalex/model-tee:latest
