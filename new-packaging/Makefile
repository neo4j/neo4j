.DEFAULT: all
.PHONY: all clean debian rpm

DISTRIBUTION ?= unstable

VERSION := $(shell bin/extract-version-from-pom ../pom.xml)

TARBALL_C := neo4j-community-$(VERSION)-unix.tar.gz
TARBALL_E := neo4j-enterprise-$(VERSION)-unix.tar.gz
ARTIFACTS := ../packaging/standalone/target
TARBALLS := $(ARTIFACTS)/$(TARBALL_C) $(ARTIFACTS)/$(TARBALL_E)
COMMUNITY_LICENSE := ../community/LICENSE.txt
COMMUNITY_POM := ../community/pom.xml
CYPHER_SHELL := ../packaging/standalone/src/main/distribution/cypher-shell
OUT := out
SHELL_SCRIPTS := ../packaging/standalone/src/main/distribution/shell-scripts/bin

COMMON_FILES := $(shell find src/common -type f)
RPM_FILES := $(shell find src/rpm -type f)
DEBIAN_FILES := $(shell find src/debian -type f)

DEBS=$(patsubst ${ARTIFACTS}/%-unix.tar.gz,${OUT}/debian/%,${TARBALLS})
RPMS=$(patsubst ${ARTIFACTS}/%-unix.tar.gz,${OUT}/rpm/%,${TARBALLS})

all: debian rpm

clean:
	rm -rf ${OUT}

debian: ${DEBS}

rpm: ${RPMS}

${OUT}/rpm/%: ${ARTIFACTS}/%-unix.tar.gz $(COMMON_FILES) $(RPM_FILES)
	rm -rf $@
	bin/build-rpm-package $@ $< ${DISTRIBUTION}

${OUT}/debian/%: ${ARTIFACTS}/%-unix.tar.gz $(COMMON_FILES) $(DEBIAN_FILES)
	rm -rf $@
	bin/build-debian-package $@ $< ${DISTRIBUTION}

$(TARBALLS):
	cd ../packaging && mvn package -DskipTests -T2C
