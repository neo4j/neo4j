/*
 * Copyright (c) "Neo4j"
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

case class BlacklistEntry(featureName: Option[String], scenarioName: String, exampleNumberOrName: Option[String], isFlaky: Boolean) {
  def isBlacklisted(scenario: Scenario): Boolean = {
    scenarioName == scenario.name &&
      (featureName.isEmpty || featureName.get == scenario.featureName) &&
      (exampleNumberOrName.isEmpty || exampleNumberOrName.get == scenario.exampleIndex.map(_.toString).getOrElse(""))
  }

  def isFlaky(scenario: Scenario): Boolean  = {
    isFlaky && isBlacklisted(scenario)
  }

  override def toString: String = {
    if (featureName.isDefined) {
      s"""Feature "${featureName.get}": Scenario "$scenarioName"""" +
        (if(exampleNumberOrName.isEmpty) "" else s""": Example "${exampleNumberOrName.get}"""")
    } else {
      s"""$scenarioName"""  // legacy version
    }
  }
}

object BlacklistEntry {
  val entryPattern: Regex = """(\??)Feature "(.*)": Scenario "([^"]*)"(: Example "(.*)")?""".r

  def apply(line: String): BlacklistEntry = {
    if (line.startsWith("?") || line.startsWith("Feature")) {
      line match {
        case entryPattern(questionMark, featureName, scenarioName, null, null) =>
          new BlacklistEntry(Some(featureName), scenarioName, None, isFlaky = questionMark.nonEmpty)
        case entryPattern(questionMark, featureName, scenarioName, _, exampleNumberOrName) =>
          new BlacklistEntry(Some(featureName), scenarioName, Some(exampleNumberOrName), isFlaky = questionMark.nonEmpty)
        case other => throw new UnsupportedOperationException(s"Could not parse blacklist entry $other")
      }

    } else new BlacklistEntry(None, line, None, isFlaky = false)
  }
}
