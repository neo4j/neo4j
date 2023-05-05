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
OS_CLASSIFIER := $(shell bin/os_classifier)
ARCH := $(shell uname -m)
DEB_ARCH := $(shell echo $(ARCH) | sed 's/x86_64/amd64/g' )

DEB_FILENAME := cypher-shell_$(DEBVERSION)_all.deb
DEB_NATIVE_FILENAME := cypher-shell_$(DEBVERSION)_$(DEB_ARCH).deb
RPM_FILENAME := cypher-shell-$(RPMVERSION).noarch.rpm
RPM_NATIVE_FILENAME := cypher-shell-$(RPMVERSION).$(ARCH).rpm

NATIVE_BINARY_FILENAME := ../cypher-shell/target/cypher-shell-$(VERSION)-$(OS_CLASSIFIER)
NATIVE_BINARY := $(OUT)/nativeBinary

DEB := $(OUT)/$(DEB_FILENAME)
DEB_NATIVE := $(OUT)/$(DEB_NATIVE_FILENAME)
RPM := $(OUT)/$(RPM_FILENAME)
RPM_NATIVE := $(OUT)/$(RPM_NATIVE_FILENAME)
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

debianNative: $(DEB_NATIVE)
.PHONY: debianNative

rpm: $(RPM) ## Build RPM package of cypher-shell
.PHONY: rpm

rpmNative: $(RPM_NATIVE)
.PHONY: rpmNative

$(NATIVE_BINARY):
	@test -f $(NATIVE_BINARY_FILENAME) || { echo "A native binary for $(OS_CLASSIFIER) does not exists. It must be build prior independent of this makefile." ; exit 1; }
	@upx -V # Check if UPX exists, we ignore the exit code below in case the binary is already compressed
	mkdir $(OUT)
	cp $(NATIVE_BINARY_FILENAME) $(OUT)/nativeBinary
	-upx $(OUT)/nativeBinary

$(DEB): $(ZIPFILE) $(COMMON_FILES) $(DEBIAN_FILES)
	mkdir -p $(OUT)/debian
	bin/build-debian-package $(OUT)/debian $< $(DISTRIBUTION)

$(DEB_NATIVE): $(NATIVE_BINARY) $(COMMON_FILES) $(DEBIAN_FILES)
	mkdir -p $(OUT)/debian
	bin/build-debian-package-native $(OUT)/debian $< $(DISTRIBUTION) $(VERSION) $(DEB_ARCH)

$(RPM): $(ZIPFILE) $(COMMON_FILES) $(RPM_FILES)
	mkdir -p $(OUT)/rpm
	bin/build-rpm-package $(OUT)/rpm $< $(DISTRIBUTION)

$(RPM_NATIVE): $(NATIVE_BINARY) $(COMMON_FILES) $(RPM_FILES)
	mkdir -p $(OUT)/rpm
	bin/build-rpm-package-native $(OUT)/rpm $< $(DISTRIBUTION) $(VERSION) $(ARCH)

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
.PHONY: debian-test

