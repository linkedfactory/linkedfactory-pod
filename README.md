[![Docker](https://img.shields.io/docker/v/linkedfactory/linkedfactory-pod?label=Docker&style=flat)](https://hub.docker.com/r/linkedfactory/linkedfactory-pod)

# Welcome to the LinkedFactory POD repository

This is the reference implementation of the 
[LinkedFactory specification](https://github.com/linkedfactory/specification).

## Run with docker
* `docker run -p 8080:8080 -v /tmp/workspace:/linkedfactory-pod/workspace linkedfactory/linkedfactory-pod`

## Building
* This is a plain Maven project
* a full build can be executed via `mvn package`

## Running 
* change to the folder `launch/equinox`
* run `mvn test -Pconfigure -DskipTests` to initialize or update a launch configuration
* run `mvn test` to (re-)start the POD instance
* The application should now be available at: [http://localhost:8080/linkedfactory/](http://localhost:8080/linkedfactory/)

## Developing
* The project can be developed with any IDE supporting Java and Scala projects
* **IDEA:** `File > Project from existing sources...`
* **Eclipse:** `File > Import > Maven > Existing Maven Projects`
