.DEFAULT: all
.PHONY: all clean debian-community debian-enterprise requires-distribution requires-version tarball-community tarball-community-extracted tarball-enterprise tarball-enterprise-extracted

ARTIFACTS=../packaging/standalone/target
OUT=out

all: tarball-community tarball-enterprise debian-community debian-enterprise

clean:
	rm -rf ${OUT}

debian-community: requires-distribution tarball-community-extracted ${OUT}/tarball-community-version.txt
	VERSION=$(cat ${OUT}/tarball-community-version.txt)
	build-debian-package ${OUT}/debian neo4j ${OUT}/tarballs/community/extracted/neo4j-community-* ${DISTRIBUTION} ${VERSION}

debian-enterprise: requires-distribution requires-version tarball-enterprise-extracted tarball-enterprise-version
	build-debian-package ${OUT}/debian neo4j-enterprise ${OUT}/tarballs/enterprise/extracted/neo4j-enterprise-* ${DISTRIBUTION} ${VERSION}

requires-distribution:
ifndef DISTRIBUTION
        $(error DISTRIBUTION is undefined)
endif

requires-version:
ifndef VERSION
        $(error VERSION is undefined)
endif

tarball-community:
	build-tarball ${ARTIFACTS}/neo4j-community-*-unix.tar.gz ${OUT}/tarballs/community

tarball-community-extracted: tarball-community
	mkdir -p ${OUT}/tarballs/community/extracted
	tar -xf ${OUT}/tarballs/community/neo4j-community-*-unix.tar.gz -C ${OUT}/tarballs/community/extracted

${OUT}/tarball-community-version.txt: tarball-community
	tarball-name-2-debian-version ${OUT}/tarballs/community/neo4j-community-*-unix.tar.gz >${OUT}/tarball-community-version.txt

tarball-enterprise:
	build-tarball ${ARTIFACTS}/neo4j-enterprise-*-unix.tar.gz ${OUT}/tarballs/enterprise

tarball-enterprise-extracted: tarball-enterprise
	mkdir -p ${OUT}/tarballs/enterprise/extracted
	tar -xf ${OUT}/tarballs/enterprise/neo4j-enterprise-*-unix.tar.gz -C ${OUT}/tarballs/enterprise/extracted
