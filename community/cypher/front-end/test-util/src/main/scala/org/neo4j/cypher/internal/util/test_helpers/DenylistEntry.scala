/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
  def acceptTransientError(scenario: Scenario): Boolean = isDenylisted(scenario) && acceptTransientError
  def acceptTransientError: Boolean

  def maybeIsFlakyPrefix: String = {
    if (isFlaky) DenylistEntry.isFlakyPrefix else ""
  }

  def maybeAcceptTransientErrorPrefix: String = {
    if (acceptTransientError) DenylistEntry.acceptTransientErrorPrefix else ""
  }
}

case class FeatureDenylistEntry(
  featureName: String,
  override val isFlaky: Boolean,
  override val acceptTransientError: Boolean
) extends DenylistEntry {
  override def isDenylisted(scenario: Scenario): Boolean = scenario.featureName == featureName
  override def toString: String = s"""${maybeIsFlakyPrefix}${maybeAcceptTransientErrorPrefix}Feature "$featureName""""
}

case class ScenarioDenylistEntry(
  featureName: Option[String],
  scenarioName: String,
  exampleNumberOrName: Option[String],
  override val isFlaky: Boolean,
  override val acceptTransientError: Boolean
) extends DenylistEntry {

  def isDenylisted(scenario: Scenario): Boolean = {
    scenarioName == scenario.name &&
    featureName.forall(_ == scenario.featureName) &&
    exampleNumberOrName.forall(_ == scenario.exampleIndex.map(_.toString).getOrElse(""))
  }

  override def toString: String = {
    val entry = featureName.map { feature =>
      val scenarioString = s"""Feature "$feature": Scenario "$scenarioName""""
      exampleNumberOrName.map { example =>
        s"""$scenarioString: Example "$example""""
      } getOrElse {
        scenarioString
      }
    } getOrElse {
      // legacy version
      scenarioName
    }
    s"${maybeIsFlakyPrefix}${maybeAcceptTransientErrorPrefix}$entry"
  }
}

object DenylistEntry {
  def isFlakyPrefix: String = "?"
  def acceptTransientErrorPrefix: String = "~"

  val entryPattern: Regex = """(\??)(~?)Feature "([^"]*)": Scenario "([^"]*)"(?:: Example "(.*)")?""".r
  val featurePattern: Regex = """^(\??)(~?)Feature "([^"]+)"$""".r

  def apply(line: String): DenylistEntry = {
    if (line.startsWith("?") || line.startsWith("~") || line.startsWith("Feature")) {
      line match {
        case entryPattern(questionMark, tilde, featureName, scenarioName, exampleNumberOrName) =>
          ScenarioDenylistEntry(
            Some(featureName),
            scenarioName,
            Option(exampleNumberOrName),
            isFlaky = questionMark.nonEmpty,
            acceptTransientError = tilde.nonEmpty
          )
        case featurePattern(questionMark, tilde, featureName) =>
          FeatureDenylistEntry(featureName, isFlaky = questionMark.nonEmpty, acceptTransientError = tilde.nonEmpty)
        case other => throw new UnsupportedOperationException(
            s"""Could not pattern match denylist entry
               |$other
               |General format is:
               |Feature "<featureName>": Scenario "<scenarioName>": Example "<exampleNumber>"
               |
               |Regexes:
               |  ${entryPattern.pattern}
               |  ${featurePattern.pattern}""".stripMargin
          )
      }

    } else {
      // Legacy case of just stating the scenario
      ScenarioDenylistEntry(None, line, None, isFlaky = false, acceptTransientError = false)
    }
  }
}
