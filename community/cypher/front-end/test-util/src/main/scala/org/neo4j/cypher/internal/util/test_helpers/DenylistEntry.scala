/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.test_helpers

import org.opencypher.tools.tck.api.Scenario

import scala.util.matching.Regex

case class DenylistEntry(featureName: Option[String], scenarioName: String, exampleNumberOrName: Option[String], isFlaky: Boolean) {
  def isDenylisted(scenario: Scenario): Boolean = {
    scenarioName == scenario.name &&
      (featureName.isEmpty || featureName.get == scenario.featureName) &&
      (exampleNumberOrName.isEmpty || exampleNumberOrName.get == scenario.exampleIndex.map(_.toString).getOrElse(""))
  }

  def isFlaky(scenario: Scenario): Boolean  = {
    isFlaky && isDenylisted(scenario)
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

object DenylistEntry {
  val entryPattern: Regex = """(\??)Feature "(.*)": Scenario "([^"]*)"(: Example "(.*)")?""".r

  def apply(line: String): DenylistEntry = {
    if (line.startsWith("?") || line.startsWith("Feature")) {
      line match {
        case entryPattern(questionMark, featureName, scenarioName, null, null) =>
          new DenylistEntry(Some(featureName), scenarioName, None, isFlaky = questionMark.nonEmpty)
        case entryPattern(questionMark, featureName, scenarioName, _, exampleNumberOrName) =>
          new DenylistEntry(Some(featureName), scenarioName, Some(exampleNumberOrName), isFlaky = questionMark.nonEmpty)
        case other => throw new UnsupportedOperationException(s"Could not parse denylist entry $other")
      }

    } else new DenylistEntry(None, line, None, isFlaky = false)
  }
}
