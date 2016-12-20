.DEFAULT: all
.PHONY: all clean debian-community debian-enterprise requires-distribution-and-version tarball-community tarball-community-extracted tarball-enterprise tarball-enterprise-extracted

ARTIFACTS=../packaging/standalone/target
OUT=out

all: tarball-community tarball-enterprise debian-community debian-enterprise

clean:
	rm -rf ${OUT}

debian-community: requires-distribution-and-version tarball-community-extracted
	build-debian-package ${OUT}/debian neo4j ${OUT}/tarballs/community/extracted/neo4j-community-* ${DISTRIBUTION} ${VERSION}

debian-enterprise: requires-distribution-and-version tarball-enterprise-extracted
	build-debian-package ${OUT}/debian neo4j-enterprise ${OUT}/tarballs/enterprise/extracted/neo4j-enterprise-* ${DISTRIBUTION} ${VERSION}

requires-distribution-and-version:
ifndef DISTRIBUTION
        $(error DISTRIBUTION is undefined)
endif
ifndef VERSION
        $(error VERSION is undefined)
endif

tarball-community:
	build-tarball ${ARTIFACTS}/neo4j-community-*-unix.tar.gz ${OUT}/tarballs/community

tarball-community-extracted: tarball-community
	mkdir -p ${OUT}/tarballs/community/extracted
	tar -xf ${OUT}/tarballs/community/neo4j-community-*-unix.tar.gz -C ${OUT}/tarballs/community/extracted

tarball-enterprise:
	build-tarball ${ARTIFACTS}/neo4j-enterprise-*-unix.tar.gz ${OUT}/tarballs/enterprise

tarball-enterprise-extracted: tarball-enterprise
	mkdir -p ${OUT}/tarballs/enterprise/extracted
	tar -xf ${OUT}/tarballs/enterprise/neo4j-enterprise-*-unix.tar.gz -C ${OUT}/tarballs/enterprise/extracted
