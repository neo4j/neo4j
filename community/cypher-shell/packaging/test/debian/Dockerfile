FROM debian:bullseye
ENV DEBIAN_FRONTEND=noninteractive

# Retry transient apt fetch failures (e.g. connection reset from deb.debian.org)
RUN echo 'Acquire::Retries "3";' > /etc/apt/apt.conf.d/80-retries

RUN apt update -qq && \
    apt install -y wget apt-transport-https gpg

# https://adoptium.net/installation/linux/
RUN wget --tries=3 --retry-connrefused --waitretry=3 -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null && \
    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt update -qq && \
    apt install -qq -y temurin-21-jdk && \
    # https://bugs.launchpad.net/ubuntu/+source/ca-certificates-java/+bug/1998065
    apt-get install -y ca-certificates-java

COPY ${DEBFILE} /tmp/
RUN apt-get install -y --no-install-recommends /tmp/${DEBFILE}

ENTRYPOINT ["/usr/bin/cypher-shell"]
