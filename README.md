# Spark-Pipelines

## Setup
1) Install docker to run spark notebook server https://www.docker.com/
2) Copy the example config and enter elasticsearch uri
```
$ cp config.json.example config.json
```
3) Build docker image
```
$ ./build_image.sh
```
4) Start spark server
```
$ ./start.sh
```
5) Copy the local url that is printed to the command line into the browser

