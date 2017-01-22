.DEFAULT: all
.PHONY: all clean debian neo4j-desktop-mac neo4j-desktop-windows tarball-community tarball-community-extracted tarball-enterprise tarball-enterprise-extracted

ARTIFACTS=../packaging/standalone/target
COMMUNITY_LICENSE=../community/LICENSE.txt
COMMUNITY_POM=../community/pom.xml
CYPHER_SHELL=../packaging/standalone/src/main/distribution/cypher-shell
DISTRIBUTION=unstable
OUT=out
SHELL_SCRIPTS=../packaging/standalone/src/main/distribution/shell-scripts/bin

TARBALLS=$(shell find $(ARTIFACTS) -name '*.tar.gz')
DEBS=$(patsubst ${ARTIFACTS}/%-unix.tar.gz,${OUT}/debian/%,${TARBALLS})

all: debian neo4j-desktop-mac neo4j-desktop-windows tarball-community tarball-enterprise

clean:
	rm -rf ${OUT}

${OUT}/neo4j-desktop/community-version.txt:
	mkdir -p ${OUT}/neo4j-desktop
	extract-version-from-pom ${COMMUNITY_POM} >${OUT}/neo4j-desktop/community-version.txt

debian: ${DEBS}

neo4j-desktop-mac: ${OUT}/neo4j-desktop/community-version.txt ${OUT}/neo4j-desktop/uberjar-filename.txt
	build-neo4j-desktop ${OUT}/neo4j-desktop-mac ${OUT}/neo4j-desktop/uberjar-filename.txt ${COMMUNITY_LICENSE} ${SHELL_SCRIPTS} ${CYPHER_SHELL} ${OUT}/neo4j-desktop/community-version.txt macosArchive ${MAC_KEYSTORE} ${MAC_KEYSTORE_PASSWORD}

neo4j-desktop-windows: ${OUT}/neo4j-desktop/community-version.txt ${OUT}/neo4j-desktop/uberjar-filename.txt
	build-neo4j-desktop ${OUT}/neo4j-desktop-windows ${OUT}/neo4j-desktop/uberjar-filename.txt ${COMMUNITY_LICENSE} ${SHELL_SCRIPTS} ${CYPHER_SHELL} ${OUT}/neo4j-desktop/community-version.txt windows ${WIN_KEYSTORE} ${WIN_KEYSTORE_PASSWORD}

tarball-community:
	mkdir -p ${OUT}/tarballs
	build-tarball ${ARTIFACTS}/neo4j-community-*-unix.tar.gz ${OUT}/tarballs

tarball-community-extracted: tarball-community
	mkdir -p ${OUT}/tarballs/extracted
	tar -xf ${OUT}/tarballs/neo4j-community-*-unix.tar.gz -C ${OUT}/tarballs/extracted

${OUT}/tarballs/community-version-debianized.txt: tarball-community
	mkdir -p ${OUT}/tarballs
	tarball-name-2-debian-version ${OUT}/tarballs/neo4j-community-*-unix.tar.gz >${OUT}/tarballs/community-version-debianized.txt

tarball-enterprise:
	build-tarball ${ARTIFACTS}/neo4j-enterprise-*-unix.tar.gz ${OUT}/tarballs

tarball-enterprise-extracted: tarball-enterprise
	mkdir -p ${OUT}/tarballs/extracted
	tar -xf ${OUT}/tarballs/neo4j-enterprise-*-unix.tar.gz -C ${OUT}/tarballs/extracted

${OUT}/tarballs/enterprise-version-debianized.txt: tarball-enterprise
	mkdir -p ${OUT}/tarballs
	tarball-name-2-debian-version ${OUT}/tarballs/neo4j-enterprise-*-unix.tar.gz >${OUT}/tarballs/enterprise-version-debianized.txt

${OUT}/neo4j-desktop/uberjar-filename.txt: ${OUT}/neo4j-desktop/community-version.txt
	mkdir -p ${OUT}/neo4j-desktop
	generate-uberjar-filename ${OUT}/neo4j-desktop/community-version.txt >${OUT}/neo4j-desktop/uberjar-filename.txt

${OUT}/debian/%: ${ARTIFACTS}/%-unix.tar.gz src/common/** src/debian/**
	build-debian-package $@ $< ${DISTRIBUTION}

${OUT}/tarballs/extracted/%: ${OUT}/tarballs/%-unix.tar.gz
	mkdir -p $(dir $@)
	tar -xf $< -C $(dir $@)

${OUT}/tarballs/%-unix.tar.gz: ${ARTIFACTS}/%-unix.tar.gz
	mkdir -p ${OUT}/tarballs
	build-tarball $< ${OUT}/tarballs
