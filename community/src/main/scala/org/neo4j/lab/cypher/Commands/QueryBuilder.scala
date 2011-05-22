package org.neo4j.lab.cypher.commands

import scala.Some

object Query {
  def apply(select:Select, start:Start) = new Query(select, start, None, None)
  def apply(select:Select, start:Start, matching:Match) = new Query(select, start, Some(matching), None)
  def apply(select:Select, start:Start, where:Where) = new Query(select, start, None, Some(where))
  def apply(select:Select, start:Start, matching:Match, where:Where) = new Query(select, start, Some(matching), Some(where))
}


case class Query(select: Select, start: Start, matching:Option[Match], where: Option[Where])

case class Select(selectItems: SelectItem*)

case class Start(startItems: StartItem*)

case class Where(clauses: Clause*)

case class Match(patterns: Pattern*)