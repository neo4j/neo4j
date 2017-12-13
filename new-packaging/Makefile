.DEFAULT: all
.PHONY: all clean debian-community debian-enterprise tarball-community tarball-community-extracted tarball-enterprise tarball-enterprise-extracted

ARTIFACTS=../packaging/standalone/target
COMMUNITY_LICENSE=../community/LICENSE.txt
COMMUNITY_POM=../community/pom.xml
DISTRIBUTION=unstable
OUT=out
SHELL_SCRIPTS=../packaging/standalone/src/main/distribution/shell-scripts/bin

all: debian-community debian-enterprise tarball-community tarball-enterprise

clean:
	rm -rf ${OUT}

debian-community: tarball-community-extracted ${OUT}/tarballs/community-version-debianized.txt
	build-debian-package ${OUT}/debian neo4j ${OUT}/tarballs/extracted/neo4j-community-* ${DISTRIBUTION} ${OUT}/tarballs/community-version-debianized.txt

debian-enterprise: tarball-enterprise-extracted  ${OUT}/tarballs/enterprise-version-debianized.txt
	build-debian-package ${OUT}/debian neo4j-enterprise ${OUT}/tarballs/extracted/neo4j-enterprise-* ${DISTRIBUTION} ${OUT}/tarballs/enterprise-version-debianized.txt

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
