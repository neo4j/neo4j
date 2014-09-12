/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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


sealed class PlannerVersion(plannerName: String) {
  val name = PlannerName.asCanonicalName(plannerName)
}

object PlannerName {
  def asCanonicalName(versionName: String) = versionName.toLowerCase
}


case object PlannerVersion {
  case object costPlanner extends PlannerVersion("cost")
  case object rulePlanner extends PlannerVersion("rule")

  def apply(versionName: String) = findVersionByExactName(CypherVersionName.asCanonicalName(versionName)).getOrElse {
    throw new SyntaxException(s"Supported versions are: ${allVersions.map(_.name).mkString(", ")}")
  }

  def findVersionByExactName(versionName: String) = allVersions.find( _.name == versionName )
  val default = costPlanner
  val allVersions = Seq(costPlanner, rulePlanner)

}
