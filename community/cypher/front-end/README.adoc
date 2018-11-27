ifdef::env-github,env-browser[:outfilesuffix: .adoc]

= The Cypher Front End Modules

image:https://travis-ci.org/opencypher/front-end.svg?branch=9.0["Build Status", link="https://travis-ci.org/opencypher/front-end"]

This repository holds a fully functional front end for the Cypher Property Graph Query Language. 
This includes a parser, an AST, semantic checking and analysis, and AST rewriting support.
Its purpose is to assist anyone wanting to build tools, or working on implementing Cypher for database systems.

It is also the shared front end for Neo4j and Morpheus.

== Overview of the included modules

* AST
** Contains the AST classes (except expressions), and the semantic analysis.

* Expressions
** This contains all expressions.

* Front End
** This module contains a prettifier for the AST, and infrastructure data structures for compiler pipelines.

* Parser
** Contains a parser for the Cypher Query Language.

* Rewriting
** This contains a set of AST rewriters that simplify and canonicalize the tree before further processing.

* Util
** Contains shared code and test utilities.

== Why do the modules have the version number in the name?

A common feature for database systems is to allow backwards compatability for older releases of the software. This makes it possible to move over systems query by query to an updated version of the database.
The names allow for two or more versions of compiler pipelines loaded at the same time. It makes it possible for us to release updated, binary incompatible versions of the front ends.

== License

The `openCypher` project is licensed under the http://www.apache.org/licenses/LICENSE-2.0[Apache license 2.0].
