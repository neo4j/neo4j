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
package org.neo4j.cypher.cucumber.synthesise.generator

import com.github.benmanes.caffeine.cache.Caffeine
import io.cucumber.datatable.DataTable
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.cucumber.glue.regular.DynamicExpectations
import org.neo4j.cypher.cucumber.glue.regular.TestConf
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.ExpectedGqlError
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.ExpectedGqlNotification
import org.neo4j.cypher.cucumber.steps.Result
import org.neo4j.cypher.cucumber.steps.Result.DoublePrecision.Exact
import org.neo4j.cypher.cucumber.steps.Result.DoublePrecision.Within
import org.neo4j.cypher.cucumber.steps.Result.Order.Ordered
import org.neo4j.cypher.cucumber.steps.Result.Order.Unordered
import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.ScenarioFilter
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.excludeTags
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.isCompatible
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.isReadQuery
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertApproxResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlError
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlWarning
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertSucceeds
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.Comment
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.CommitTransaction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.CreateCsvFile
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.Execute
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControl
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControlInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecuted
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecutedInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.OpenTransaction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.QueryExecution
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedScenario
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedStep
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RegisterProcedure
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RegisterUserFunction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.SetParams
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.SetupExecution
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.SideEffects
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.TestExecution
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.TransactionHandling
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UpdateClause
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.kernel.api.QueryLanguage

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE_NEW
import java.nio.file.StandardOpenOption.WRITE

import scala.collection.View
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.reflect.ClassTag
import scala.util.Try

trait ScenarioGenerator extends ScenarioRenderer {

  final def generate(): Unit = {
    require(!Files.exists(args.exportDirectory), s"${args.exportDirectory} must not exist")
    generateScenarios(filteredScenarios).iterator
      .foreach(scenario => writeScenario(args.exportDirectory, scenario))
  }
  def name: String
  def args: CucumberSalad.Ingredients

  def filter: Filter = Filter(args.parser, Seq.empty)
    .scenario(isCompatible(args.targetConf))
    .scenario(excludeTags("@fails", s"@fails:$name", "@ignore", "@ignore:generator", s"@ignore:generator:$name"))
    .scenario(s => !s.tags.exists(_.startsWith("@conf:")))

  def generateScenarios(filteredScenarios: View[RecordedScenario]): IterableOnce[GeneratedScenario]
  private def filteredScenarios: View[RecordedScenario] = args.source.view.filter(filter.build)
}

case class GeneratedScenario(
  name: String,
  steps: Seq[RecordedStep],
  featurePath: Path,
  comment: String,
  tags: Set[String],
  examples: Option[DataTable] = None
)

case class ParsedQuery(query: String, statement: String, ast: Statement)

class CachingParser(version: CypherVersion) {

  private val preparser = new PreParser(CypherConfiguration.fromConfig(Config.defaults(
    GraphDatabaseInternalSettings.enable_experimental_cypher_versions,
    java.lang.Boolean.TRUE
  )))
  private val cache = Caffeine.newBuilder().maximumSize(256).build[String, ParsedQuery]().asMap()

  def parse(cypher: String): ParsedQuery = cache.computeIfAbsent(cypher, q => doParse(version, q))

  private def doParse(version: CypherVersion, cypher: String): ParsedQuery =
    try {
      val statement = preparser.preParse(cypher, version).statement
      val ast =
        AstParserFactory(version)(statement, Neo4jCypherExceptionFactory(statement, None), None, Seq())
          .singleStatement()
      ParsedQuery(cypher, statement, ast)
    } catch {
      case throwable: Throwable => throw new RuntimeException(
          s""""Failed to parse query during scenario generation.
             |Cypher version: $version
             |Query:
             |$cypher
             |""".stripMargin,
          throwable
        )
    }
}

case class Filter(parser: CachingParser, predicates: Seq[RecordedScenario => Boolean] = Seq.empty) {

  def and(pred: ScenarioFilter): Filter = copy(predicates = predicates.appended(pred))
  def scenario(pred: ScenarioFilter): Filter = and(pred)

  def steps[T <: RecordedStep : ClassTag](pred: Seq[T] => Boolean): Filter =
    and(scenario => pred(Filter.steps[T](scenario)))

  def queries[T <: QueryExecution : ClassTag](pred: Seq[ParsedQuery] => Boolean): Filter =
    and(s => pred(Filter.steps[T](s).map(e => parser.parse(e.cypher))))
  def setupQueries(pred: Seq[ParsedQuery] => Boolean): Filter = queries[SetupExecution](pred)
  def testQueries(pred: Seq[ParsedQuery] => Boolean): Filter = queries[TestExecution](pred)

  def testsReadQueries: Filter = steps[SideEffects](_.forall(_.expected.isEmpty))
    .testQueries(queries => queries.nonEmpty && queries.forall(isReadQuery))

  /** Keeps scenarios whose every query parses, so later predicates and generation can parse freely. */
  def allQueriesParse: Filter =
    and(s => Try(Filter.steps[QueryExecution](s).foreach(e => parser.parse(e.cypher))).isSuccess)
  def build: RecordedScenario => Boolean = scenario => predicates.forall(_.apply(scenario))
}

object Filter {
  type ScenarioFilter = RecordedScenario => Boolean
  type QueryFilter = ParsedQuery => Boolean

  def excludeTags(tags: String*)(scenario: RecordedScenario): Boolean = !tags.exists(scenario.tags.contains)

  def isCompatible(conf: TestConf): ScenarioFilter = {
    val expectations = new DynamicExpectations(conf)
    scenario => !expectations.fails(scenario.tags) && !expectations.ignore(scenario.tags)
  }

  def steps[T <: RecordedStep](scenario: RecordedScenario)(implicit ct: ClassTag[T]): Seq[T] =
    scenario.steps.collect { case step if ct.runtimeClass.isAssignableFrom(step.getClass) => step.asInstanceOf[T] }

  def containsAst[T <: ASTNode : ClassTag](query: ParsedQuery): Boolean =
    query.ast.folder.treeFindByClass[T].nonEmpty

  def doNotContainAst[T <: ASTNode : ClassTag](query: ParsedQuery): Boolean = !containsAst[T](query)

  def isNotCommand(query: ParsedQuery): Boolean = !query.ast.folder.treeExists {
    case _: SchemaCommand | _: AdministrationCommand | _: CommandClause => true
  }
  def isReadQuery(query: ParsedQuery): Boolean = isNotCommand(query) && doNotContainAst[UpdateClause](query)
}

trait ScenarioRenderer {

  def writeScenario(dir: Path, scenario: GeneratedScenario): Unit = {
    val path = dir.resolve(scenario.featurePath)
    if (!Files.exists(path)) {
      Files.createDirectories(path.getParent)
      val header =
        s"""# This file contains generated test cases.
           >Feature: ${path.getFileName.toString.replace(".feature", "")}
           >
           >""".stripMargin('>')
      Files.writeString(path, header, WRITE, CREATE_NEW)
    }
    Files.writeString(path, renderScenario(scenario), WRITE, APPEND)
  }

  private def renderScenario(scenario: GeneratedScenario): CharSequence = {
    val res = new StringBuilder()
    scenario.comment.linesIterator.foreach(l => res.append("  # ").append(l).append("\n"))
    scenario.tags.foreach(tag => res.append("  ").append(tag).append("\n"))
    if (scenario.examples.isEmpty) res.append("  Scenario: ")
    else res.append("  Scenario Template: ")
    res.append(scenario.name).append("\n")
    scenario.steps.view.map(renderStep).foreach(step =>
      res.append(step.linesIterator.mkString("    ", "\n      ", "\n"))
    )
    scenario.examples.foreach { examples =>
      res.append("    Examples:").append("\n")
        .append(examples.toString.linesIterator.mkString("      ", "\n      ", "\n"))
    }
    res.append("\n")
  }

  private def tripleQuote(action: String, value: String): String =
    action + "\n\"\"\"\n" + value.trim + "\n\"\"\""

  private def render(heading: String, value: DataTable): String = {
    heading + "\n" + (if (value.isEmpty) "|" else value.toString.trim)
  }

  private def procedureVersionSuffix(supportedLanguages: Set[QueryLanguage]): String =
    supportedLanguages.toSeq match {
      case Seq(QueryLanguage.CYPHER_5)  => " only in Cypher 5"
      case Seq(QueryLanguage.CYPHER_25) => " only in Cypher 25"
      case _                            => ""
    }

  private def renderStep(step: RecordedStep): String = step match {
    case execution: QueryExecution => execution match {
        case HavingExecuted(cypher)         => tripleQuote("And having executed:", cypher)
        case HavingExecutedInOpenTx(cypher) => tripleQuote("And having executed, in open tx:", cypher)
        case Execute(cypher)                => tripleQuote("When executing query:", cypher)
        case ExecuteInOpenTx(cypher)        => tripleQuote("When executing query, in open tx:", cypher)
        case ExecuteControl(cypher)         => tripleQuote("When executing control query:", cypher)
        case ExecuteControlInOpenTx(cypher) => tripleQuote("When executing control query, in open tx:", cypher)
      }
    case handling: TransactionHandling => handling match {
        case OpenTransaction   => "Given an open transaction"
        case CommitTransaction => "Then open transaction should commit without errors"
      }
    case SetParams(params) =>
      render("And parameters are:", DataTable.create(params.toSeq.map(t => java.util.List.of(t._1, t._2)).asJava))
    case AssertResults(expected, _) if expected.isEmpty => "Then the result should be empty"
    case AssertSucceeds                                 => "Then the query should not fail"
    case AssertResults(expected, Result.Single(a)) =>
      val orderString = (a.rowOrder, a.listOrder) match {
        case (Ordered, Ordered)     => ", in order"
        case (Unordered, Ordered)   => ", in any order"
        case (Ordered, Unordered)   => ", in order (ignoring element order for lists)"
        case (Unordered, Unordered) => " (ignoring element order for lists)"
      }
      val precisionString = a.doublePrecision match {
        case Within(epsilon) => s", to within $epsilon"
        case Exact           => ""
      }
      render(s"Then the result should be$orderString$precisionString:", expected)
    case AssertResults(_, Result.ParallelOverride(_, _)) => ??? // TODO
    case AssertApproxResults(_, _)                       => ??? // TODO
    case AssertGqlWarning(ExpectedGqlNotification(table, _)) =>
      render(s"Then notifications should be raised:", table)
    case AssertGqlError(ExpectedGqlError(table, _)) =>
      render(s"Then an error should be raised:", table)
    case SideEffects(expected) if expected.isEmpty =>
      "And no side effects"
    case SideEffects(expected) =>
      render("And the side effects should be:", expected)
    case Comment(comment) =>
      s"# $comment"
    case RegisterProcedure(signature, results, supportedLanguages) =>
      render(s"And there exists a procedure $signature${procedureVersionSuffix(supportedLanguages)}:", results)
    case CreateCsvFile(param, table) =>
      render(s"And there exists a CSV file with URL as ${"$" + param}, with rows:", table)
    case RegisterUserFunction(name) =>
      s"And the $name function is registered"
  }
}
