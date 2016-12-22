.DEFAULT: all
.PHONY: all clean debian-community debian-enterprise requires-distribution tarball-community tarball-community-extracted tarball-enterprise tarball-enterprise-extracted

ARTIFACTS=../packaging/standalone/target
OUT=out

all: tarball-community tarball-enterprise debian-community debian-enterprise

clean:
	rm -rf ${OUT}

debian-community: requires-distribution tarball-community-extracted ${OUT}/tarballs/community-version-debianized.txt
	build-debian-package ${OUT}/debian neo4j ${OUT}/tarballs/extracted/neo4j-community-* ${DISTRIBUTION} ${OUT}/tarballs/community-version-debianized.txt

debian-enterprise: requires-distribution tarball-enterprise-extracted  ${OUT}/tarballs/enterprise-version-debianized.txt
	build-debian-package ${OUT}/debian neo4j-enterprise ${OUT}/tarballs/extracted/neo4j-enterprise-* ${DISTRIBUTION} ${OUT}/tarballs/enterprise-version-debianized.txt

requires-distribution:
ifndef DISTRIBUTION
	$(error DISTRIBUTION is undefined)
endif

tarball-community:
	build-tarball ${ARTIFACTS}/neo4j-community-*-unix.tar.gz ${OUT}/tarballs

tarball-community-extracted: tarball-community
	mkdir -p ${OUT}/tarballs/extracted
	tar -xf ${OUT}/tarballs/neo4j-community-*-unix.tar.gz -C ${OUT}/tarballs/extracted

${OUT}/tarballs/community-version-debianized.txt: tarball-community
	tarball-name-2-debian-version ${OUT}/tarballs/neo4j-community-*-unix.tar.gz >${OUT}/tarballs/community-version-debianized.txt

tarball-enterprise:
	build-tarball ${ARTIFACTS}/neo4j-enterprise-*-unix.tar.gz ${OUT}/tarballs

tarball-enterprise-extracted: tarball-enterprise
	mkdir -p ${OUT}/tarballs/extracted
	tar -xf ${OUT}/tarballs/neo4j-enterprise-*-unix.tar.gz -C ${OUT}/tarballs/extracted

${OUT}/tarballs/enterprise-version-debianized.txt: tarball-enterprise
	tarball-name-2-debian-version ${OUT}/tarballs/neo4j-enterprise-*-unix.tar.gz >${OUT}/tarballs/enterprise-version-debianized.txt
