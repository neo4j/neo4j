.DEFAULT: all

DISTRIBUTION ?= unstable

ifeq ($(VERSION),)
# If VERSION was not provided as an argument (used in builds where we don't have xmlstarlet), get it from pom
VERSION := $(shell ../bin/extract-version-from-pom ../pom.xml || { echo >&2 "Error reading version from pom (perhaps xmlstarlet is not installed), aborting"; kill $$PPID; })
endif

DEBVERSION := $(shell bin/convert-to-debian-version "$(VERSION)")
RPMVERSION := $(shell bin/convert-to-rpm-version "$(VERSION)")

ZIPFILE := ../cypher-shell/target/cypher-shell-$(VERSION).zip
OUT := out

COMMON_FILES := $(shell find src/common -type f)
RPM_FILES := $(shell find src/rpm -type f)
DEBIAN_FILES := $(shell find src/debian -type f)
DEB_FILENAME := cypher-shell_$(DEBVERSION)_all.deb
RPM_FILENAME := cypher-shell-$(RPMVERSION).noarch.rpm

DEB := $(OUT)/$(DEB_FILENAME)
RPM := $(OUT)/$(RPM_FILENAME)
DEB_TEST := tmp/debian-test
RPM_TEST := tmp/rpm-test

clean:
	rm -rf $(OUT)
	rm -rf $(RPM_TEST)
	rm -rf $(DEB_TEST)
.PHONY: clean

# Build packages

all: debian rpm ## Build Debian and RPM packages of cypher-shell
.PHONY: all

debian: $(DEB) ## Build Debian package of cypher-shell
.PHONY: debian

rpm: $(RPM) ## Build RPM package of cypher-shell
.PHONY: rpm

$(DEB): $(ZIPFILE) $(COMMON_FILES) $(DEBIAN_FILES)
	mkdir -p $(OUT)/debian
	bin/build-debian-package $(OUT)/debian $< $(DISTRIBUTION)

$(RPM): $(ZIPFILE) $(COMMON_FILES) $(RPM_FILES)
	mkdir -p $(OUT)/rpm
	bin/build-rpm-package $(OUT)/rpm $< $(DISTRIBUTION)

$(ZIPFILE):
	cd ../../.. && mvn package --projects org.neo4j:cypher-shell --also-make -DskipTests

# Tests

rpm-test: $(RPM) ## Test RPM package (requires Docker)
	rm -rf $(RPM_TEST)
	bin/test-rpm-package $(RPM) $(RPM_TEST)
.PHONY: rpm-test

debian-test: $(DEB) ## Test Debian package (requires Docker)
	rm -rf $(DEB_TEST)
	bin/test-debian-package $(DEB) $(DEB_TEST)
.PHONY: rpm-test

