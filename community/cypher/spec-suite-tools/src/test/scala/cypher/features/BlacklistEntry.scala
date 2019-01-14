/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package cypher.features

import org.opencypher.tools.tck.api.Scenario

import scala.util.matching.Regex

case class BlacklistEntry(featureName: Option[String], scenarioName: String) {
  def isBlacklisted(scenario: Scenario): Boolean = {
    scenarioName == scenario.name && (featureName.isEmpty || featureName.get == scenario.featureName)
  }

  override def toString: String = {
    if (featureName.isDefined) {
      s"""Feature "${featureName.get}": Scenario "$scenarioName""""
    } else {
      s"""$scenarioName"""  // legacy version
    }
  }
}

object BlacklistEntry {
  val entryPattern: Regex = """Feature "(.*)": Scenario "(.*)"""".r

  def apply(line: String): BlacklistEntry = {
    if (line.startsWith("Feature")) {
      line match {
        case entryPattern(featureName, scenarioName) => new BlacklistEntry(Some(featureName), scenarioName)
        case other => throw new UnsupportedOperationException(s"Could not parse blacklist entry $other")
      }

    } else new BlacklistEntry(None, line)
  }
}
