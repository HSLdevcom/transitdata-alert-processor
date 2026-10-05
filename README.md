# transitdata-alert-processor [![CI/CD](https://github.com/HSLdevcom/transitdata-alert-processor/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-alert-processor/actions/workflows/ci-cd.yml)

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

Application for creating GTFS-RT Service Alerts from internal service alert messages.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

Requires Java 25. `transitdata-common` is resolved from GitHub Packages, so `GITHUB_ACTOR` and `GITHUB_TOKEN`
(a token with `read:packages`) must be set, or configured in `~/.m2/settings.xml`.

### Locally

- `./mvnw compile`
- `./mvnw test` runs the unit tests
- `./mvnw verify` also runs the integration tests (`*IT`, needs Docker for Testcontainers)
- `./mvnw spotless:apply` formats the code; CI only runs `spotless:check`
- `./mvnw package` builds `target/transitdata-alert-processor.jar`

### Docker image

- Run [this script](build-image.sh) to build the Docker image (passes `GITHUB_TOKEN` as a build secret)


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   
