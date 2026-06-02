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
package org.neo4j.cypher.cucumber.steps

import io.cucumber.datatable.DataTable
import io.cucumber.scala.EN
import io.cucumber.scala.ScalaDsl
import org.apache.commons.io.IOUtils
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.ExpectedGqlError
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.ExpectedGqlNotification
import org.neo4j.cypher.cucumber.steps.Result.DoublePrecision.Exact
import org.neo4j.cypher.cucumber.steps.Result.DoublePrecision.Within
import org.neo4j.cypher.cucumber.steps.Result.Order.Ordered
import org.neo4j.cypher.cucumber.steps.Result.Order.Unordered
import org.neo4j.cypher.cucumber.steps.Result.Single
import org.neo4j.kernel.api.QueryLanguage

import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

/**
 * Step definitions of all Cypher Cucumber steps (from the .feature test files).
 */
trait CypherCucumberSteps extends InOpenTxCypherCucumberSteps {

  // Given
  // =====

  Given("an empty graph") {}

  Given("any graph") {
    loadNamedGraph("binary-tree-1")
  }

  Given("""the {word} graph""") { (graphName: String) =>
    loadNamedGraph(graphName)
  }

  Given("""parameters are:""") { (params: DataTable) =>
    parametersAre(params.asMap().asScala.toMap)
  }

  Given("^there exists a procedure (?!.*? only in Cypher \\d)(.+):$") { (signature: String, results: DataTable) =>
    registerProcedure(signature, results)
  }

  Given("^there exists a procedure (.+?) only in Cypher (\\d+):$") {
    (signature: String, version: Int, results: DataTable) =>
      registerProcedure(signature, results, versionToQueryLanguage(version))
  }

  Given("there exists a CSV file with URL as ${word}, with rows:") { (param: String, content: DataTable) =>
    givenCsvFile(param, content)
  }

  Given("the {word} function is registered") { (func: String) =>
    registerUserFunction(func)
  }

  // And
  // ===

  And("having executed:") { (query: String) => havingExecuted(query) }

  // When
  // ====

  When("executing query:") { (query: String) => executingQuery(query) }

  When("""executing control query:""") { (query: String) =>
    executingControlQuery(query)
  }

  // Then
  // ====

  Then("the result should be, in order:") { (expected: DataTable) =>
    resultShouldBe(expected, Result.InOrder)
  }

  Then("the approximate result should be {int} rows, in order:") { (nbrOfResults: Int, expected: DataTable) =>
    approximateResultShouldBe(expected, nbrOfResults)
  }

  Then("the result should be, in order, to within {double}:") { (epsilon: Double, expected: DataTable) =>
    resultShouldBe(expected, Result.Assertion(Ordered, Ordered, Within(epsilon)))
  }

  Then("""the result should be, in order \(or any order if executed in parallel):""") { (expected: DataTable) =>
    resultShouldBe(expected, Result.ParallelOverride(Result.InOrder, Result.InAnyOrder))
  }

  Then("""the result should be, in order \(or any order ignoring element order for lists if executed in parallel):""") {
    (expected: DataTable) =>
      resultShouldBe(expected, Result.ParallelOverride(Result.InAnyOrder, Result.InAnyOrderWithUnorderedLists))
  }

  Then(
    """the result should be, in order \(or any order if executed in parallel) \(ignoring element order for lists):"""
  ) { (expected: DataTable) =>
    resultShouldBe(
      expected,
      Result.ParallelOverride(Result.InOrderWithUnorderedLists, Result.InAnyOrderWithUnorderedLists)
    )
  }

  Then(
    """the result should be, in any order \(and ignoring element order for lists if executed in parallel):"""
  ) {
    (expected: DataTable) =>
      resultShouldBe(expected, Result.ParallelOverride(Result.InAnyOrder, Result.InAnyOrderWithUnorderedLists))
  }

  Then("the result should be, in any order:") { (expected: DataTable) =>
    resultShouldBe(expected, Result.InAnyOrder)
  }

  Then("the result should be, in any order, to within {double}:") { (epsilon: Double, expected: DataTable) =>
    resultShouldBe(expected, Result.InAnyOrder.copy(doublePrecision = Within(epsilon)))
  }

  Then("""the result should be, in order \(ignoring element order for lists):""") { (expected: DataTable) =>
    resultShouldBe(expected, Result.InOrderWithUnorderedLists)
  }

  Then("""the result should be \(ignoring element order for lists):""") { (expected: DataTable) =>
    resultShouldBe(expected, Result.InAnyOrderWithUnorderedLists)
  }

  Then("the result should be empty") {
    resultShouldBe(DataTable.emptyDataTable(), Result.InOrder)
  }

  Then("no side effects") {
    sideEffectsShouldBe(DataTable.emptyDataTable())
  }

  Then("the side effects should be:") { (expected: DataTable) =>
    sideEffectsShouldBe(expected)
  }

  // Needs table including headers:
  // - code, the expected GQL code, comma separated to allow a set of codes.
  // - classification, the expected error classification.
  // - description, error status description, supports in-lining regex: ${regex:.*}.
  //
  Then("an error should be raised:") { (table: DataTable) =>
    errorShouldBeRaised(ExpectedGqlError(table))
  }

  Then("no notifications should be raised") {
    notificationsShouldBeRaised(ExpectedGqlNotification.empty)
  }

  Then("notifications should be raised:") { (table: DataTable) =>
    notificationsShouldBeRaised(ExpectedGqlNotification(table))
  }

  // Supports JsonUnit syntax. For example:
  // {
  //   "a": "${json-unit.regex}[A-Z]+}",
  //   "b": "${json-unit.any-string}"
  // }
  // See https://github.com/lukas-krecan/JsonUnit
  Then("query log should contain:") { (log: String) =>
    queryLogShouldContain(log)
  }

  private def readNamedGraphCypher(name: String): String = {
    IOUtils.resourceToString(s"graphs/$name/$name.cypher", StandardCharsets.UTF_8, getClass.getClassLoader)
  }
  def loadNamedGraph(name: String): Unit = havingExecuted(readNamedGraphCypher(name))
  def parametersAre(params: Map[String, String]): Unit
  def registerProcedure(signature: String, results: DataTable): Unit

  def registerProcedure(signature: String, results: DataTable, supportedLanguages: Set[QueryLanguage]): Unit =
    registerProcedure(signature, results)

  def registerUserFunction(name: String): Unit

  private def versionToQueryLanguage(version: Int): Set[QueryLanguage] = version match {
    case 5  => Set(QueryLanguage.CYPHER_5)
    case 25 => Set(QueryLanguage.CYPHER_25)
    case _  => throw new IllegalArgumentException(s"Unsupported Cypher version: $version")
  }
  def givenCsvFile(urlParam: String, content: DataTable): Unit
  def havingExecuted(cypher: String): Unit
  def executingQuery(cypher: String): Unit
  def executingControlQuery(cypher: String): Unit
  private def resultShouldBe(expected: DataTable, a: Result.Assertion): Unit = resultShouldBe(expected, Single(a))
  def resultShouldBe(expected: DataTable, assert: Result.Assertions): Unit
  def approximateResultShouldBe(expected: DataTable, rowCount: Int): Unit
  def sideEffectsShouldBe(expected: DataTable): Unit
  def errorShouldBeRaised(hierarchy: ExpectedGqlError): Unit
  def notificationsShouldBeRaised(expectedWarnings: ExpectedGqlNotification): Unit
  def queryLogShouldContain(expectedJsonLog: String): Unit
}

object CypherCucumberSteps {

  case class ErrorDescription(code: Seq[String], classification: Seq[String], descriptionTemplate: String)
  case class ExpectedGqlError(table: DataTable, errors: Seq[ErrorDescription])
  case class NotificationDescription(code: String, descriptionTemplate: String, optional: Boolean)
  case class ExpectedGqlNotification(table: DataTable, warnings: Seq[NotificationDescription])

  object ExpectedGqlError {
    private val Headers = java.util.List.of("code", "classification", "description")

    def apply(table: DataTable): ExpectedGqlError = table.row(0) match {
      case Headers => ExpectedGqlError(
          table = table,
          errors = table.cells().asScala.view
            .drop(1) // Headers
            .map(row => ErrorDescription(row.get(0).split(','), row.get(1).split(','), row.get(2)))
            .toSeq
        )
      case headers => throw new IllegalArgumentException(s"Unrecognized headers: " + headers)
    }
  }

  object ExpectedGqlNotification {
    private val Headers = java.util.List.of("code", "description")

    def empty: ExpectedGqlNotification = ExpectedGqlNotification(DataTable.create(java.util.List.of(Headers)))

    def apply(table: DataTable): ExpectedGqlNotification = table.row(0) match {
      case Headers => ExpectedGqlNotification(
          table = table,
          warnings = table.cells().asScala.view
            .drop(1) // Headers
            .map { row =>
              val rawCode = row.get(0)
              val isOptional = rawCode.endsWith("?")
              val code = if (isOptional) rawCode.substring(0, rawCode.length - 1) else rawCode
              val desc = row.get(1)
              NotificationDescription(code, desc, isOptional)
            }
            .toSeq
        )
      case headers => throw new IllegalArgumentException(s"Unrecognized headers: " + headers)
    }
  }
}

trait InOpenTxCypherCucumberSteps extends ScalaDsl with EN {

  // Given
  // =====

  Given("an open transaction") { openTransaction() }

  // And
  // ===

  And("having executed, in open tx:") { (query: String) => havingExecutedInOpenTx(query) }

  // When
  // ====

  When("executing query, in open tx:") { (query: String) => executingQueryInOpenTx(query) }
  When("executing control query, in open tx:") { (query: String) => executingControlQueryInOpenTx(query) }

  When("open transaction is commited and re-opened") {
    commitOpenTx()
    openTransaction()
  }

  // Then
  // ====

  Then("open transaction should commit without errors") {
    commitOpenTx()
  }

  def openTransaction(): Unit
  def havingExecutedInOpenTx(cypher: String): Unit
  def executingQueryInOpenTx(cypher: String): Unit
  def executingControlQueryInOpenTx(cypher: String): Unit
  def commitOpenTx(): Unit
}

object Result {
  sealed trait Order

  object Order {
    case object Ordered extends Order
    case object Unordered extends Order
  }

  sealed trait DoublePrecision

  object DoublePrecision {
    case object Exact extends DoublePrecision
    case class Within(epsilon: Double) extends DoublePrecision
  }

  case class Assertion(rowOrder: Order, listOrder: Order, doublePrecision: DoublePrecision)
  val InOrder = Assertion(Ordered, Ordered, Exact)
  val InOrderWithUnorderedLists = Assertion(Ordered, Unordered, Exact)
  val InAnyOrder = Assertion(Unordered, Ordered, Exact)
  val InAnyOrderWithUnorderedLists = Assertion(Unordered, Unordered, Exact)

  sealed trait Assertions {
    def configure(runtime: String): Assertion
  }

  case class Single(assertion: Assertion) extends Assertions {
    final override def configure(runtime: String): Assertion = assertion
  }

  case class ParallelOverride(default: Assertion, parallel: Assertion) extends Assertions {

    final override def configure(runtime: String): Assertion = runtime match {
      case "parallel" => parallel
      case _          => default
    }
  }
}
