package org.neo4j.lab.cypher.commands

case class Query(select: Select, from: From, where: Option[Where] = None)

case class Select(selectItems: SelectItem*)

case class From(fromItems: FromItem*)

case class Where(clauses: Clause*)

