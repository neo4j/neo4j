FROM debian:stretch-backports
ENV DEBIAN_FRONTEND noninteractive

COPY ${DEBFILE} /tmp/

RUN apt-get update -qq && \
    # Because of a bug we need to install java before cypher-shell
    # https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=924897
    apt-get install -y openjdk-11-jre-headless && \
    apt-get install -y --no-install-recommends /tmp/${DEBFILE}

ENTRYPOINT ["/usr/bin/cypher-shell"]
