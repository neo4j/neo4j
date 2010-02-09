Neo4j Shell is a generic neo4j command shell which you can use to do simple
interactions in a running graph database service instance or (by pointing it
to a neo4j store directory) load a graph and browse around in it.

The Shell manual is located at:
http://wiki.neo4j.org/content/Shell

Quick start for the standalone version:

Start Shell by invoking a suitable script from the
bin/ folder of the distribution.

Example arguments for remote:
    -port 1337
    -port 1337 -name shell
    ...or no arguments
Example arguments for local:
    -path /path/to/db
    -path /path/to/db -readonly

