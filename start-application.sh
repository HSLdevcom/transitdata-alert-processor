#!/bin/bash

if [[ "${DEBUG_ENABLED}" = true ]]; then
  java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -Xms256m -Xmx1024m -jar /usr/app/transitdata-alert-processor.jar
else
  java -Xms256m -Xmx1024m -jar /usr/app/transitdata-alert-processor.jar
fi