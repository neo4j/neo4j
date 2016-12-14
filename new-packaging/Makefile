.DEFAULT: all
.PHONY: all clean debian-community tarball-community tarball-community-extracted

ARTIFACTS='../packaging/standalone/target'
OUT=out
STABILITY=unstable
VERSION=3.0.9.SNAPSHOT

all: tarball-community debian-community

clean:
	rm -rf ${OUT}

debian-community: tarball-community-extracted
	build-debian-package.sh ${OUT}/debian-community src/debian/template/common src/debian/template/community ${VERSION} ${STABILITY} ${OUT}/tarballs/community/extracted

tarball-community:
	build-tarball.sh ${ARTIFACTS}/neo4j-community-*-unix.tar.gz ${OUT}/tarballs/community

tarball-community-extracted: tarball-community
	mkdir -p ${OUT}/tarballs/community/extracted
	tar -xf ${OUT}/tarballs/community/neo4j-community-*-unix.tar.gz -C ${OUT}/tarballs/community/extracted
