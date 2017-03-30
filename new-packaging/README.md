## Pre-Requisites
You need `xmlstarlet`, `dos2unix`

You also need `cypher-shell` packaged

You can either checkout `cypher-shell` and build it yourself:
```
cd /tmp
git clone https://github.com/neo4j/cypher-shell.git
make out/cypher-shell.zip
cp out/cypher-shell.zip /path/to/neo4j
```
Or download the zip file from teamcity

and then,

`cp out/cypher-shell.zip /path/to/neo4j`

## Package `neo4j`

From `/path/to/neo4j directory` run `mvn install`

`mvn install -DskipTests -DskipCypher -T2C`

From `/path/to/neo4j/new-packaging`

## Debian

`PATH=$(pwd)/bin:$PATH DISTRIBUTION=stable make clean debian`

## RPM

`PATH=$(pwd)/bin:$PATH DISTRIBUTION=stable make clean rpm`

## Windows Deskop installer

`PATH=$(pwd)/bin:/path/to/install4j/bin:$PATH DISTRIBUTION=stable make clean neo4j-desktop-windows`
