#!/bin/bash
# Parse the version from a neo4j tarball like 'neo4j-community-3.2.0-SNAPSHOT-unix.tar.gz'
# and parses out
# version: 3.2.0-SNAPSHOT
# deb_version: 3.2.0~SNAPSHOT
# rpm_version: 3.2.0
# version_label: SNAPSHOT (or empty if no label present)
parse_version_from_tarball() {
  local filename edition_version_extension version_extension
  filename=$(basename "${1}")
  edition_version_extension=${filename#*-}
  version_extension=${edition_version_extension#*-}

  version=${version_extension%-*}
  parse_version "${version}"
}

parse_version() {
  version="${1}"
  # Deb-files do not include the epoch in their filename so provide an
  # epoch-less variant for Make
  deb_version=${version/-/"~"}
  # Need the 1: prefix (epoch) to make sure that newer debian packages
  # are in fact considered newer, to overrule bad version numbers in
  # old versions. We also add -1 to get a unique filename because
  # epochs are not included in deb-filenames.
  deb_version_full="1:${deb_version}"
  rpm_version=${version%-*}
  version_label=${version#*-}
  # if no label
  if [[ "${version_label}" = "${rpm_version}" ]]; then
    version_label=""
  fi
  # https://fedoraproject.org/wiki/Packaging:Versioning
  if [ -z ${version_label} ]; then
    # Release version
    rpm_release="1"
  else
    # Pre-release version
    rpm_release="0.${version_label}.1"
  fi

  export version
  export version_label
  export deb_version
  export deb_version_full
  export rpm_version
  export rpm_release
}

# Parse the version from a neo4j tarball like 'neo4j-community-3.2.0-SNAPSHOT-unix.tar.gz'
# package_name: neo4j (if community)
# package_name: neo4j-enterprise (if enterprise)
parse_pkgname() {
  # Figure if community or enterprise
  if [[ $(basename "${1}") =~ "enterprise" ]]; then
    package_name="neo4j-enterprise"
  else
    package_name="neo4j"
  fi
  export package_name
}

# Parse the license from a neo4j tarball like 'neo4j-community-3.2.0-SNAPSHOT-unix.tar.gz'
# license: GPLv3 (if community)
# license: AGPLv3 (if enterprise)
parse_license() {
  # Figure if community or enterprise
  if [[ $(basename ${1}) =~ "enterprise" ]]; then
    license="AGPLv3"
  else
    license="GPLv3"
  fi
  export license
}
