# Cypher Shell RPM and Debian packaging

## How to build and test

### RPM package

- Run `make rpm` to build RPM package.
- You will find the RPM package in 
  `out/cypher-shell-{rpm-version}.{rpm-release}.noarch.rpm`
- Run `make rpm-test` to test the RPM package. This requires docker.
   
### Debian package

- Run `make debian` to build Debian package.
- You will find the Debian package in 
  `cypher-shell_{debian-version}_all.deb`
- Run `make rpm-test` to test the RPM package. This requires docker.
