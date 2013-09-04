Neo4j Desktop
-------------

Neo4j Desktop is a self-contained GUI application for launching neo4j server.

This project can build two artifacts:

o `mvn package` builds an über jar containing all dependencies and neo4j desktop
o `mvn package -Dinstall4j.home=<path-to-install4j>` additionally builds a windows installer in `target/install4j` that packages the über jar and includes a bundled JRE.

We brand the Windows installer and the Windows application "Neo4j Community".

To build the windows installer, you first need to install install4j and obtain a license. Additionally, copy a JRE for bundling to `<path-to-install4j/jres>`.
The best way to get a JRE in the correct format is by using the install4j GUI.



