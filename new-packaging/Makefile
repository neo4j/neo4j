.DEFAULT: all
.PHONY: all clean debian neo4j-desktop-mac neo4j-desktop-windows rpm
# Just a test
ARTIFACTS=../packaging/standalone/target
COMMUNITY_LICENSE=../community/LICENSE.txt
COMMUNITY_POM=../community/pom.xml
CYPHER_SHELL=../packaging/standalone/src/main/distribution/cypher-shell
DISTRIBUTION=unstable
OUT=out
SHELL_SCRIPTS=../packaging/standalone/src/main/distribution/shell-scripts/bin

TARBALLS=$(shell find $(ARTIFACTS) -name '*.tar.gz')
DEBS=$(patsubst ${ARTIFACTS}/%-unix.tar.gz,${OUT}/debian/%,${TARBALLS})
RPMS=$(patsubst ${ARTIFACTS}/%-unix.tar.gz,${OUT}/rpm/%,${TARBALLS})

all: debian neo4j-desktop-mac neo4j-desktop-windows rpm

clean:
	rm -rf ${OUT}

debian: ${DEBS}

neo4j-desktop-mac: ${OUT}/neo4j-desktop/community-version.txt ${OUT}/neo4j-desktop/uberjar-filename.txt
	build-neo4j-desktop ${OUT}/neo4j-desktop-mac ${OUT}/neo4j-desktop/uberjar-filename.txt ${COMMUNITY_LICENSE} ${SHELL_SCRIPTS} ${CYPHER_SHELL} ${OUT}/neo4j-desktop/community-version.txt macosArchive ${MAC_KEYSTORE} ${MAC_KEYSTORE_PASSWORD}

neo4j-desktop-windows: ${OUT}/neo4j-desktop/community-version.txt ${OUT}/neo4j-desktop/uberjar-filename.txt
	build-neo4j-desktop ${OUT}/neo4j-desktop-windows ${OUT}/neo4j-desktop/uberjar-filename.txt ${COMMUNITY_LICENSE} ${SHELL_SCRIPTS} ${CYPHER_SHELL} ${OUT}/neo4j-desktop/community-version.txt windows ${WIN_KEYSTORE} ${WIN_KEYSTORE_PASSWORD}

rpm: ${RPMS}

${OUT}/neo4j-desktop/community-version.txt:
	mkdir -p ${OUT}/neo4j-desktop
	extract-version-from-pom ${COMMUNITY_POM} >${OUT}/neo4j-desktop/community-version.txt

${OUT}/neo4j-desktop/uberjar-filename.txt: ${OUT}/neo4j-desktop/community-version.txt
	mkdir -p ${OUT}/neo4j-desktop
	generate-uberjar-filename ${OUT}/neo4j-desktop/community-version.txt >${OUT}/neo4j-desktop/uberjar-filename.txt

${OUT}/rpm/%: ${ARTIFACTS}/%-unix.tar.gz src/common/** src/rpm/**
	build-rpm-package $@ $< ${DISTRIBUTION}

${OUT}/debian/%: ${ARTIFACTS}/%-unix.tar.gz src/common/** src/debian/**
	build-debian-package $@ $< ${DISTRIBUTION}
