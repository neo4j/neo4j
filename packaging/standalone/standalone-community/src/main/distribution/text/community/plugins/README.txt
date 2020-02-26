Neo4j Extensions
================

Neo4j can be extended by writing custom code which can be invoked directly from Cypher,
as described in the developer manual at https://neo4j.com/docs/developer-manual/3.5/extending-neo4j/

These extensions are user defined procedures, user defined functions or security plugins. They are written in Java
and compiled into jar files. They can be deployed to the database by dropping a JAR file into the $NEO4J_HOME/plugins
directory on each standalone or clustered server. The database must be re-started on each server to pick up new procedures.

Configuring Procedures
----------------------

There are two configuration settings in $NEO4J_HOME/conf/neoj4.conf that are relevant when deploying procedures
or user defined functions:

* dbms.security.procedures.unrestricted
* dbms.security.procedures.whitelist

These are described in more detail in the documentation at
https://neo4j.com/docs/operations-manual/3.5/security/securing-extensions/

Sandboxing
----------

Neo4j provides sandboxing to ensure that procedures do not inadvertently use insecure APIs. For example, when writing
custom code it is possible to access Neo4j APIs that are not publicly supported, and these internal APIs are subject
to change, without notice. Additionally, their use comes with the risk of performing insecure actions. The sandboxing
functionality limits the use of extensions to publicly supported APIs, which exclusively contain safe operations,
or contain security checks.

For example:

    # Example sandboxing
    dbms.security.procedures.unrestricted=my.extensions.example,my.procedures.*

White listing
-------------

White listing can be used to allow loading only a few extensions from a larger library.
The configuration setting dbms.security.procedures.whitelist is used to name certain procedures that should be
available from a library. It defines a comma-separated list of procedures that are to be loaded.
The list may contain both fully-qualified procedure names, and partial names with the wildcard *.

For example, to only load a subset of the APOC library of procedures:

    # Example white listing
    dbms.security.procedures.whitelist=apoc.coll.*,apoc.load.*

