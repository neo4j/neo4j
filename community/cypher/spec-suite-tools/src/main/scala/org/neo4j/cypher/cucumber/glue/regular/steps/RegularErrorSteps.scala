/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.cucumber.glue.regular.steps

import io.cucumber.datatable.DataTable
import org.junit.jupiter.api.Assertions.fail
import org.neo4j.cypher.cucumber.glue.regular.TestConf
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularCypherSteps.GqlFailure
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularCypherSteps.QueryExecution
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularCypherSteps.QueryFailure
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularCypherSteps.QueryResults
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularCypherSteps.doDescribeFailure
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularCypherSteps.originalError
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularCypherSteps.unexpectedFailure
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularCypherSteps.unexpectedSuccess
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularErrorSteps.IgnoredWarnings
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularErrorSteps.describeErrorCodes
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularErrorSteps.describeWarnings
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularErrorSteps.errorHierarchy
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularErrorSteps.errorsMatch
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularErrorSteps.keptForComparison
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularErrorSteps.warningsMatch
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.ExpectedGqlError
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.ExpectedGqlNotification
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.NotificationDescription
import org.neo4j.cypher.testing.api.GqlNotification
import org.neo4j.gqlstatus.ErrorGqlStatusObject

import java.util.regex.Pattern

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.matching.Regex

trait RegularErrorSteps { this: CypherCucumberSteps =>
  def lastExecutionResult: QueryExecution
  protected def describePlan(actual: QueryExecution): String
  def conf: TestConf

  final override def errorShouldBeRaised(expected: ExpectedGqlError): Unit = lastExecutionResult match {
    case result: QueryResults => unexpectedSuccess(result, conf, describePlan(result))
    case failure: QueryFailure =>
      val actual = errorHierarchy(originalError(failure.cause))
      if (!errorsMatch(actual, expected)) {
        fail(
          s"""Actual errors:
             >${describeErrorCodes(actual)}
             >
             >Did not match expected errors:
             >${expected.table}
             >
             >${doDescribeFailure(failure)}
             >
             >Plan:
             >${describePlan(failure)}
             >""".stripMargin('>')
        )
      }
  }

  override def notificationsShouldBeRaised(expected: ExpectedGqlNotification): Unit = lastExecutionResult match {
    case result: QueryResults =>
      val actual = result.results.qqlStatusObjects
      if (!warningsMatch(actual, expected)) {
        fail(
          s"""Actual warnings (ignored codes: ${IgnoredWarnings.mkString(", ")}):
             >${describeWarnings(actual.filter(keptForComparison(expected, _)))}
             >
             >Did not match expected warnings:
             >${expected.table}
             >
             >Query:
             >${result.query}
             >""".stripMargin('>')
        )
      }

    case failure: QueryFailure => unexpectedFailure(failure, conf, describePlan(failure))
  }
}

object RegularErrorSteps {

  private val InlineRegexPattern = "\\$\\{regex:(?<exp>[^}]+?)\\}".r

  val IgnoredWarnings = Set(
    // Ignore result codes to make it more convenient to write scenarios.
    "00001",
    "00000",
    "02000",
    // Ignore missing node label/rel type/property key warnings, they are unstable because we re-use the db and very common.
    "01N51",
    "01N52",
    "01N50"
  )

  // Ignored codes are filtered out by default, but a code that a scenario explicitly expects is kept so the scenario
  // can assert on it (e.g. a missing-label warning that is otherwise ignored).
  private def keptForComparison(expected: ExpectedGqlNotification, warning: GqlNotification): Boolean = {
    val expectedCodes = expected.warnings.map(_.code).toSet
    expectedCodes.contains(warning.code) || !IgnoredWarnings.contains(warning.code)
  }

  @tailrec
  def errorHierarchy(throwable: AnyRef, acc: Seq[GqlFailure] = Seq.empty): Seq[GqlFailure] = throwable match {
    case e: org.neo4j.driver.exceptions.Neo4jException => errorHierarchy(
        e.gqlCause().orElse(null),
        acc.appended(GqlFailure(
          code = e.gqlStatus(),
          classification = e.classification().map[String](_.toString).orElse("UNKNOWN"),
          description = e.statusDescription(),
          message = e.getMessage
        ))
      )
    case o: ErrorGqlStatusObject => errorHierarchy(
        o.cause().orElse(null),
        acc.appended(GqlFailure(
          code = o.gqlStatus(),
          classification = o.getClassification.toString,
          description = o.statusDescription(),
          message = o.getMessage
        ))
      )
    case _ => acc
  }

  private def errorsMatch(actual: Seq[GqlFailure], expected: ExpectedGqlError): Boolean = expected match {
    case ExpectedGqlError(_, expectedErrors) =>
      actual.size == expectedErrors.size &&
      actual.zip(expectedErrors).forall { case (actual, expected) =>
        val codeMatches = expected.code.contains(actual.code)
        val classificationMatches = expected.classification.contains(actual.classification)
        val descriptionMatches = actual.description == expected.descriptionTemplate ||
          buildRegexFromTemplate(expected.descriptionTemplate).matches(actual.description)
        codeMatches && classificationMatches && descriptionMatches
      }
  }

  private def warningsMatch(actual: Seq[GqlNotification], allExpected: ExpectedGqlNotification): Boolean = {
    val actualInteresting = actual.filter(keptForComparison(allExpected, _))
    val (optionalExpected, expected) = allExpected.warnings.partition(_.optional)

    // Filter out optional warnings
    val actualWithoutOptional = optionalExpected.foldLeft(actualInteresting) {
      case (actualLeft, optExpect) => actualLeft.filterNot(w => warningMatch(w, optExpect))
    }

    // Remaining warnings should match exactly (currently requires that expected are ordered)
    actualWithoutOptional.size == expected.size &&
    actualWithoutOptional
      .sortBy(w => (w.code, w.statusDescription))
      .zip(expected)
      .forall { case (actual, expected) => warningMatch(actual, expected) }
  }

  private def warningMatch(actual: GqlNotification, expected: NotificationDescription): Boolean = {
    val codeMatches = actual.code == expected.code
    val descriptionMatches = actual.statusDescription == expected.descriptionTemplate ||
      buildRegexFromTemplate(expected.descriptionTemplate).matches(actual.statusDescription)
    codeMatches && descriptionMatches
  }

  // Template can be a string like: "It was the year ${regex:\d\d\d\d}."
  private def buildRegexFromTemplate(template: String): Regex = {
    val regex = new StringBuilder(template.length)
    var lastIndex = 0
    for (matchResult <- InlineRegexPattern.findAllMatchIn(template)) {
      val literalPart = template.substring(lastIndex, matchResult.start)
      if (literalPart.nonEmpty) {
        regex.append(Pattern.quote(literalPart))
      }
      regex.append(matchResult.group("exp"))
      lastIndex = matchResult.end
    }
    if (lastIndex < template.length) {
      regex.append(Pattern.quote(template.substring(lastIndex)))
    }
    regex.result().r
  }

  def describeErrorCodes(failures: Seq[GqlFailure]): String = DataTable.create(
    failures
      .map(e => java.util.List.of(e.code, e.classification, e.description))
      .prepended(java.util.List.of("code", "classification", "description"))
      .asJava
  ).toString

  def describeWarnings(failures: Seq[GqlNotification]): String = DataTable.create(
    failures
      .map(e => java.util.List.of(e.code, e.statusDescription))
      .prepended(java.util.List.of("code", "description"))
      .asJava
  ).toString
}
