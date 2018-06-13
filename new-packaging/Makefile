.DEFAULT: all

DISTRIBUTION ?= unstable

VERSION := $(shell bin/extract-version-from-pom ../pom.xml)
DEBVERSION := $(shell bin/convert-to-debian-version "$(VERSION)")
RPMVERSION := $(shell bin/convert-to-rpm-version "$(VERSION)")

ARTIFACTS := ../packaging/standalone/target
TARBALL_C := $(ARTIFACTS)/neo4j-community-$(VERSION)-unix.tar.gz
TARBALL_E := $(ARTIFACTS)/neo4j-enterprise-$(VERSION)-unix.tar.gz
TARBALLS := $(TARBALL_C) $(TARBALL_E)
OUT := out

COMMON_FILES := $(shell find src/common -type f)
RPM_FILES := $(shell find src/rpm -type f)
DEBIAN_FILES := $(shell find src/debian -type f)

DEB_C := $(OUT)/debian/neo4j_$(DEBVERSION)_all.deb
DEB_E := $(OUT)/debian/neo4j-enterprise_$(DEBVERSION)_all.deb

RPM_C := $(OUT)/rpm/neo4j-$(RPMVERSION).noarch.rpm
RPM_E := $(OUT)/rpm/neo4j-enterprise-$(RPMVERSION).noarch.rpm

all: debian rpm
.PHONY: all

clean:
	rm -rf $(OUT)
.PHONY: clean

debian: $(DEB_C) $(DEB_E)
.PHONY: debian

rpm: $(RPM_C) $(RPM_E)
.PHONY: rpm

$(DEB_C): $(TARBALL_C) $(COMMON_FILES) $(DEBIAN_FILES)
	rm -rf out/debian/build_community
	bin/build-debian-package out/debian/build_community $< $(DISTRIBUTION)

$(DEB_E): $(TARBALL_E) $(COMMON_FILES) $(DEBIAN_FILES)
	rm -rf out/debian/build_enterprise
	bin/build-debian-package out/debian/build_enterprise $< $(DISTRIBUTION)

$(RPM_C): $(TARBALL_C) $(COMMON_FILES) $(RPM_FILES)
	rm -rf out/rpm/build_community
	bin/build-rpm-package out/rpm/build_community $< $(DISTRIBUTION)

$(RPM_E): $(TARBALL_E) $(COMMON_FILES) $(RPM_FILES)
	rm -rf out/rpm/build_enterprise
	bin/build-rpm-package out/rpm/build_enterprise $< $(DISTRIBUTION)

$(TARBALLS):
	cd ../packaging && mvn package -DskipTests -T2C
