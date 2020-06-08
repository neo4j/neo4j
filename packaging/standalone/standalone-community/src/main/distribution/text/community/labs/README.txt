Neo4j Labs Plugins
================

The jar files in this directory are plugins supported by Neo4j Labs.

Neo4j Labs is a collection of the latest innovations in graph technology. These projects are designed and developed by the Neo4j team as a way to test functionality and extensions of our product offerings. A project typically either graduates to being maintained as a formal Neo4j project or is deprecated with source made available publicly.

Labs projects are supported via the online community. They are actively developed and maintained, but we don't provide any SLAs or guarantees around backwards compatibility and deprecation.

The Labs projects license description is in this same directory.
For more information, see https://neo4j.com/labs

Enabling Plugins
----------------------

To enable these plugins, move the jar files to the plugins directory.
The DBMS needs to be restarted for the jar to be activated.

By default only procedures that don't use low-level APIs are enabled. To enable all APOC procedures, add the following line in $NEO4J_HOME/conf/neo4j.conf:

dbms.security.procedures.unrestricted=apoc.*
