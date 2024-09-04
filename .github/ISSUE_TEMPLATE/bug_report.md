---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

## Guidelines

Please note that GitHub issues are only meant for bug reports/feature requests. 
If you have questions on how to use Neo4j, please ask on [StackOverflow](https://stackoverflow.com/questions/tagged/neo4j) instead of creating an issue here.

Before creating a new issue, please check whether someone else has raised the same issue. You may be able to add context to that issue instead of duplicating the report. However, each issue should also only be focussed on a _single_ problem, so do not describe new problems within an existing thread - these are very hard to track and manage, and your problem may be ignored. Finally, do not append comments to closed issues; if the same problem re-occurs, open a new issue, and include a link to the old one.

To help us understand your issue, please specify important details, primarily:

- Neo4j version: X.Y.Z
- Operating system: (for example Windows 95/Ubuntu 16.04)
- Installation method: (for example Neo4j Desktop, tar/zip/deb/rpm package, apt/yum repository, docker, compiled source)
- API/Driver: (for example Cypher/Java API/Python driver vX.Y.Z)
- **Steps to reproduce**
- Expected behavior
- Actual behavior

Additionally, include (as appropriate) log-files, stacktraces, and other debug output.

## Example bug report

I discovered that when I mount `data/` to a volume on my host, and then stop the container, the `write.lock` is not removed as well as a number of other files not being cleaned up properly.

**Neo4j Version:** 3.0M03
**Operating System:** Ubuntu 15.10
**Installation Method:** Docker
**API:** Docker

### Steps to reproduce
1. Pull the image: `docker pull neo4j/neo4j:3.0.0-M03`
2. Create a directory on the host that will be a mount for the data: `mkdir /home/neo4j-data`
3. Start a new container that mounts to this directory: `docker run -d --name neo4j-test -p 7474:7474 -v /home/neo4j-data:/data neo4j/neo4j:3.0.0-M03`
4. Navigate to the `write.lock` file located in the directory: `/data/databases/graph.db/schema/label/lucene/labelStore/1/`
5. Stop the container: `docker stop neo4j-test`

### Expected behavior
`write.lock` should be removed now.

### Actual behavior
`write.lock` is still present and preventing access by other programs.
