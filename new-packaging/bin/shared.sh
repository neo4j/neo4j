# Parse the version from a neo4j tarball like 'neo4j-community-3.2.0-SNAPSHOT-unix.tar.gz'
# and parses out
# version: 3.2.0-SNAPSHOT
# deb_version: 3.2.0.SNAPSHOT
# rpm_version: 3.2.0
# version_label: SNAPSHOT (or empty if no label present)
parse_version() {
  local filename=$(basename ${1})
  local edition_version_extension=${filename#*-}
  local version_extension=${edition_version_extension#*-}
  version=${version_extension%-*}
  deb_version=${version/-/.}
  rpm_version=${version%-*}
  version_label=${version#*-}
  # if no label
  if [[ ${version_label} = ${rpm_version} ]]; then
    version_label=""
  fi
}

# Parse the version from a neo4j tarball like 'neo4j-community-3.2.0-SNAPSHOT-unix.tar.gz'
# package_name: neo4j (if community)
# package_name: neo4j-enterprise (if enterprise)
parse_pkgname() {
  # Figure if community or enterprise
  if [[ $(basename ${1}) =~ "enterprise" ]]; then
    package_name="neo4j-enterprise"
  else
    package_name="neo4j"
  fi
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
}
