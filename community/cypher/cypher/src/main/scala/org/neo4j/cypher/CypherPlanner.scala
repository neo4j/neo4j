/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

sealed abstract class CypherPlanner(plannerName: String) {
  val name = CypherOptionName.asCanonicalName(plannerName)
}

object CypherPlanner {

  case object default extends CypherPlanner("default")
  case object cost extends CypherPlanner("cost")
  case object idp extends CypherPlanner("idp")
  case object dp extends CypherPlanner("dp")
  case object rule extends CypherPlanner("rule")

  def apply(name: String) = findPlannerByExactName(CypherOptionName.asCanonicalName(name)).getOrElse {
    throw new SyntaxException(s"Supported planners are: ${allPlanners.map(_.name).mkString(", ")}")
  }

  def findPlannerByExactName(name: String) = allPlanners.find( _.name == name )

  val allPlanners = Seq(default, cost, idp, dp, rule)
}
