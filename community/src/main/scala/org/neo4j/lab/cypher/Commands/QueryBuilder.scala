package org.neo4j.lab.cypher.commands

case class Query(select: Select, from: List[FromItem] = Nil, where: Option[Where] = None)

case class Select(selectItems: SelectItem*)

case class Where(clauses: Clause*)

