FROM fedora:33

COPY ${RPMFILE} /tmp/

RUN yum --assumeyes install /tmp/${RPMFILE}

ENTRYPOINT ["/usr/bin/cypher-shell"]
