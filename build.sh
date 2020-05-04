#!/bin/bash

VERSION="2.1.0"

docker build . -t lepsalex/model-tee:$VERSION -t lepsalex/model-tee:latest
docker push lepsalex/model-tee:$VERSION
docker push lepsalex/model-tee:latest
