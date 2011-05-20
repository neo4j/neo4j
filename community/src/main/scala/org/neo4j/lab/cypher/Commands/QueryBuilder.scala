package org.neo4j.lab.cypher.commands

import scala.Some

case class Query(select: Select, start: Start, matching:Option[Match], where: Option[Where])

case class Select(selectItems: SelectItem*)

case class Start(startItems: StartItem*)

case class Where(clauses: Clause*)

case class Match(patterns: Pattern*)