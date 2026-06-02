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
package org.neo4j.cypher.cucumber.glue.obfuscator

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.Inject
import io.cucumber.datatable.DataTable
import io.cucumber.scala.Scenario
import net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson
import net.javacrumbs.jsonunit.core.Option.IGNORING_EXTRA_FIELDS
import org.apache.commons.io.input.ReversedLinesFileReader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Assumptions.assumeFalse
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.cucumber.glue.obfuscator.ObfuscatorSteps.LoggedQuery
import org.neo4j.cypher.cucumber.glue.regular.Executors
import org.neo4j.cypher.cucumber.glue.regular.Expectations
import org.neo4j.cypher.cucumber.glue.regular.InjectedTestConf
import org.neo4j.cypher.cucumber.glue.regular.SingletonInjector
import org.neo4j.cypher.cucumber.glue.regular.TestConf
import org.neo4j.cypher.cucumber.glue.regular.steps.RegularCypherSteps
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps
import org.neo4j.cypher.cucumber.steps.Result
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.internal.helpers.Exceptions

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.function.Consumer

import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using

final class ObfuscatorSteps @Inject() (
  conf: TestConf,
  executors: Executors,
  expectations: Expectations
) extends CypherCucumberSteps {

  private[this] val inner = new RegularCypherSteps(conf, executors, expectations)
  private[this] var taggedQueries = Map.empty[String, String]
  private[this] var expectedLogs = Map.empty[String, String]
  private[this] var lastTag: String = ""
  private[this] var scenarioTag: String = _
  private[this] var start: Instant = _

  Before { (scenario: Scenario) =>
    assumeFalse(expectations.fails(scenario), "Ignoring scenario because of @fails tag")
    assumeFalse(expectations.ignore(scenario), "Ignoring scenario because of @ignore tag")
    this.start = Instant.now()
    inner.initialize(scenario)
    this.scenarioTag = "// obfuscator-test-tag " + scenario.getUri.toString + ":" + scenario.getLine
  }

  After(10) {
    assumeTrue(inner.getDbmsAccessor != null)
    val logItemsByTag = queryLogFiles()
      .flatMap(logPath => logLines(logPath))
      .groupBy(_.tag)
    taggedQueries.foreach { case (tag, query) =>
      val actualLog = logItemsByTag.getOrElse(tag, Seq.empty)
      if (actualLog.size < 2) {
        // TODO Query log sometimes contains three entries, feels like a bug?
        val message =
          s"""Unexpected log line size, found ${actualLog.size}
             |Hint: You might need to change obfuscator-test-server-logs.xml, have a look at the JsonTemplateLayout maxStringLength and SizeBasedTriggeringPolicy size""".stripMargin
        fail(describe(message, query, actualLog))
      }
      if (!query.contains("*")) {
        actualLog.foreach { logItem =>
          val rewrittenObfuscatedQuery = logItem.query.replace("******", "null")
          Try(ObfuscatorSteps.parse(rewrittenObfuscatedQuery)) match {
            case Success(_) =>
            case Failure(error) => fail(describe(
                s"""Expected to successfully parse obfuscated query when replacing ****** with null
                   |
                   |Obfuscated Query:
                   |${logItem.query}
                   |
                   |Replacement Query:
                   |$rewrittenObfuscatedQuery
                   |
                   |Parsing Error:
                   |${Exceptions.stringify(error)}
                   |""".stripMargin,
                query,
                actualLog
              ))
          }
        }
      }
    }
    expectedLogs.foreach { case (tag, expectedLog) =>
      val actualLog = logItemsByTag.getOrElse(tag, Seq.empty)
      if (actualLog.size < 2) {
        fail(describe(s"Unexpected log line size, found ${actualLog.size}", expectedLog, actualLog))
      }

      def jsonMatches(expected: String): Consumer[LoggedQuery] = actual =>
        assertThatJson(actual.rawLog)
          .when(IGNORING_EXTRA_FIELDS)
          .isEqualTo(expected)

      assertThat(actualLog.asJava).anySatisfy(jsonMatches(expectedLog))

    }
  }

  After(1) { inner.after() }

  private def logLines(queryLogPath: Path): Seq[LoggedQuery] = {
    var result = Seq.empty[LoggedQuery]
    Using.resource(ReversedLinesFileReader.builder().setBufferSize(2048).setFile(queryLogPath.toFile).get()) { reader =>
      var line = reader.readLine(); var done = false
      while (line != null && !done) {
        val logLine = ObfuscatorSteps.Json.readTree(line)

        if (line.contains(scenarioTag)) {
          val loggedQuery = logLine.path("query").asText()
          val loggedTag = loggedQuery.substring(loggedQuery.indexOf(scenarioTag))
          val logItem = LoggedQuery(line, loggedQuery, loggedTag, logLine.path("event").asText(), line)
          result = result.appended(logItem)
        }

        val time = Instant.from(ObfuscatorSteps.TimeFormatter.parse(logLine.path("time").asText()))
        done = time.isBefore(start)
        line = reader.readLine()
      }
    }
    result
  }

  private def queryLogFiles(): Seq[Path] = {
    val logsDir = inner.getDbmsAccessor.dbms.database.getDependencyResolver
      .resolveDependency(classOf[Config])
      .get(GraphDatabaseSettings.logs_directory)
    Files.list(logsDir)
      .filter(_.toString.contains("query.log"))
      .sorted()
      .toList.asScala.toSeq
  }

  private def tagQuery(cypher: String): String = {
    val tag = scenarioTag + ":" + UUID.randomUUID().toString
    val taggedQuery = cypher + "\n" + tag
    taggedQueries = taggedQueries.updated(tag, taggedQuery)
    lastTag = tag
    taggedQuery
  }

  private def describe(message: String, query: String, logItems: Seq[LoggedQuery]): String = {
    s"""$message
       |
       |Query:
       |$query
       |
       |Found Log lines:
       |${logItems.view.map(_.logLine).mkString("\n")}
       |""".stripMargin
  }

  override def parametersAre(params: Map[String, String]): Unit = inner.parametersAre(params)

  override def registerProcedure(sign: String, results: DataTable): Unit = inner.registerProcedure(sign, results)
  override def registerUserFunction(name: String): Unit = inner.registerUserFunction(name)
  override def givenCsvFile(urlParam: String, content: DataTable): Unit = inner.givenCsvFile(urlParam, content)
  override def havingExecuted(q: String): Unit = inner.havingExecuted(tagQuery(q))
  override def executingQuery(q: String): Unit = inner.executingQuery(tagQuery(q))
  override def executingControlQuery(q: String): Unit = inner.executingControlQuery(tagQuery(q))
  override def openTransaction(): Unit = inner.openTransaction()
  override def havingExecutedInOpenTx(q: String): Unit = inner.havingExecutedInOpenTx(tagQuery(q))
  override def executingQueryInOpenTx(q: String): Unit = inner.executingQueryInOpenTx(tagQuery(q))
  override def executingControlQueryInOpenTx(q: String): Unit = inner.executingControlQueryInOpenTx(tagQuery(q))
  override def commitOpenTx(): Unit = inner.commitOpenTx()

  // Failing queries are silently ignored
  override def errorShouldBeRaised(expectedError: CypherCucumberSteps.ExpectedGqlError): Unit =
    taggedQueries = taggedQueries.removed(lastTag)

  override def notificationsShouldBeRaised(expectedWarnings: CypherCucumberSteps.ExpectedGqlNotification): Unit =
    taggedQueries = taggedQueries.removed(lastTag)

  // For log assertions we compare the entire log to given partial json log
  override def queryLogShouldContain(expectedJsonLog: String): Unit = {
    expectedLogs += (lastTag -> expectedJsonLog)
  }

  // We don't check regular assertions here

  override def resultShouldBe(expected: DataTable, assert: Result.Assertions): Unit = {}
  override def approximateResultShouldBe(expected: DataTable, rowCount: Int): Unit = {}
  override def sideEffectsShouldBe(expected: DataTable): Unit = {}
}

object ObfuscatorSteps {
  val Json = new ObjectMapper()
  val QueryLanguage = CypherVersion.Cypher25
  val Parser = AstParserFactory.apply(QueryLanguage)

  val PreParser = new PreParser(CypherConfiguration.fromConfig(Config.defaults(
    GraphDatabaseSettings.default_language,
    GraphDatabaseSettings.CypherVersion.Cypher25
  )))
  val TimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ")

  def parse(cypher: String): Statement = {
    val preparsed = PreParser.preParse(cypher, QueryLanguage)
    Parser.apply(preparsed.statement, Neo4jCypherExceptionFactory(cypher, Some(preparsed.options.offset)), None)
      .singleStatement()
  }

  case class LoggedQuery(logLine: String, query: String, tag: String, event: String, rawLog: String)

  object Conf extends InjectedTestConf {
    final val FactoryName = "org.neo4j.cypher.cucumber.glue.obfuscator.ObfuscatorSteps$Conf$ObjectFactory"

    override val conf: TestConf = TestConf.withCypher25(TestConf(
      neo4jConf = Map(
        "db.logs.query.enabled" -> "verbose",
        "db.logs.query.obfuscate_literals" -> "true"
      ),
      useBolt = true,
      serverLogsConfResource = Some("/test/logs/conf/obfuscator-test-server-logs.xml"),
      additionalTagContext = Set("obfuscation"),
      maxDbmsReuse = Some(256) // Tests seems to get slower with dbms re-use for unknown reason, so let's force restarts
    ))
    final class ObjectFactory extends SingletonInjector(injector)
  }
}
