# transitdata-alert-processor [![Test and create Docker image](https://github.com/HSLdevcom/transitdata-alert-processor/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-alert-processor/actions/workflows/test-and-build.yml)

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

Application for creating GTFS-RT Service Alerts from internal service alert messages.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- `mvn compile`
- `mvn package`

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   
