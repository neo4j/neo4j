## Guidelines

Please note that GitHub issues are only meant for bug reports/feature requests.  If you have questions on how to use Neo4j, please ask on [StackOverflow](http://stackoverflow.com/questions/tagged/neo4j) instead of creating an issue here.

If you want to make a feature request then there is no guideline, so feel free to stop reading and open an issue. If you have a bug report however, please continue reading.
To help us understand your issue, please specify important details, primarily:

- Neo4j version: X.Y.Z
- Operating system: (for example Windows 95/Ubuntu 16.04)
- API/Driver: (for example Cypher/Java API/Python driver vX.Y.Z)
- **Steps to reproduce**
- Expected behavior
- Actual behavior

Additionally, include (as appropriate) log-files, stacktraces, and other debug output.

## Example bug report

I discovered that when I mount `data/` to a volume on my host, and then stop the container, the `write.lock` is not removed as well as a number of other files not being cleaned up properly.

**Neo4j Version:** 3.0M03  
**Operating System:** Ubuntu 15.10  
**API:** Docker

### Steps to reproduce
1. Pull the image: `docker pull neo4j/neo4j:3.0.0-M03`
2. Create a directory on the host that will be a mount for the data: `mkdir /home/neo4j-data`
3. Start a new container that mounts to this directory: `docker run -d --name neo4j-test -p 7474:7474 -v /home/neo4j:/data neo4j/neo4j:3.0.0-M03`
4. Navigate to the `write.lock` file located in the directory: `/data/databases/graph.db/schema/label/lucene/labelStore/1/`
5. Stop the container: `docker stop neo4j-test`

### Expected behavior
`write.lock` should be removed now.

### Actual behavior
`write.lock` is still present and preventing access by other programs.
