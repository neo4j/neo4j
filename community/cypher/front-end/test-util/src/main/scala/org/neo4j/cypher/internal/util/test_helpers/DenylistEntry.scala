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

trait DenylistEntry {
  def isDenylisted(scenario: Scenario): Boolean
  def isFlaky(scenario: Scenario): Boolean = isDenylisted(scenario) && isFlaky
  def isFlaky: Boolean
}

case class FeatureDenylistEntry(
  featureName: String
) extends DenylistEntry {
  override def isDenylisted(scenario: Scenario): Boolean = scenario.featureName == featureName
  override def isFlaky: Boolean = false
}

case class ScenarioDenylistEntry(
  featureName: Option[String],
  scenarioName: String,
  exampleNumberOrName: Option[String],
  override val isFlaky: Boolean
) extends DenylistEntry {

  def isDenylisted(scenario: Scenario): Boolean = {
    scenarioName == scenario.name &&
    (featureName.isEmpty || featureName.get == scenario.featureName) &&
    (exampleNumberOrName.isEmpty || exampleNumberOrName.get == scenario.exampleIndex.map(_.toString).getOrElse(""))
  }

  override def toString: String = {
    if (featureName.isDefined) {
      s"""Feature "${featureName.get}": Scenario "$scenarioName"""" +
        (if (exampleNumberOrName.isEmpty) "" else s""": Example "${exampleNumberOrName.get}"""")
    } else {
      s"""$scenarioName""" // legacy version
    }
  }
}

object DenylistEntry {
  val entryPattern: Regex = """(\??)Feature "(.*)": Scenario "([^"]*)"(: Example "(.*)")?""".r
  val featurePattern: Regex = """^Feature "([^"]+)"$""".r

  def apply(line: String): DenylistEntry = {
    if (line.startsWith("?") || line.startsWith("Feature")) {
      line match {
        case entryPattern(questionMark, featureName, scenarioName, null, null) =>
          ScenarioDenylistEntry(Some(featureName), scenarioName, None, isFlaky = questionMark.nonEmpty)
        case entryPattern(questionMark, featureName, scenarioName, _, exampleNumberOrName) =>
          ScenarioDenylistEntry(
            Some(featureName),
            scenarioName,
            Some(exampleNumberOrName),
            isFlaky = questionMark.nonEmpty
          )
        case featurePattern(featureName) =>
          FeatureDenylistEntry(featureName)
        case other => throw new UnsupportedOperationException(s"Could not parse denylist entry $other")
      }

    } else ScenarioDenylistEntry(None, line, None, isFlaky = false)
  }
}
