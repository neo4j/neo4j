# Image for running expect against Cypher Shell.
# This image is created on the fly in some Cypher Shell integration tests.
FROM eclipse-temurin:17-jre-alpine
RUN apk --no-cache add expect unzip

COPY cypher-shell.zip cypher-shell.zip
RUN unzip cypher-shell.zip
RUN mv cypher-shell-* cypher-shell

COPY expect/* /

ENTRYPOINT ["tail", "-f", "/dev/null"]