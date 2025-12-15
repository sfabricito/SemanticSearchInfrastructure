#!/bin/bash
# $1: username

docker login

cd embedding-api
docker build -t "$1/embedding-api" .
docker push "$1/embedding-api"

