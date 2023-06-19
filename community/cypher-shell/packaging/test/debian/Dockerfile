FROM debian:bullseye
ENV DEBIAN_FRONTEND noninteractive

COPY ${DEBFILE} /tmp/

RUN apt-get update -qq && \
    # https://bugs.launchpad.net/ubuntu/+source/ca-certificates-java/+bug/1998065
    apt-get install -y ca-certificates-java && \

    # Because of a bug we need to install java before cypher-shell
    # https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=924897
    apt-get install -y openjdk-17-jre-headless && \
    apt-get install -y --no-install-recommends /tmp/${DEBFILE}

ENTRYPOINT ["/usr/bin/cypher-shell"]
