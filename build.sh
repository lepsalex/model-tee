#!/bin/bash

VERSION="3.6.2"

docker build . -t lepsalex/model-tee:$VERSION -t lepsalex/model-tee:latest
docker push lepsalex/model-tee:$VERSION
docker push lepsalex/model-tee:latest
