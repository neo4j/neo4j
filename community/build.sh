#!/bin/sh
export MAVEN_OPTS="-Xss2m -Xmx1G"
mvn clean install -DskipTests