/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher.feature.steps

import java.util

import _root_.cucumber.api.DataTable
import cypher.FeatureSuiteTest
import cypher.cucumber.db.DatabaseConfigProvider.cypherConfig
import cypher.cucumber.db.DatabaseLoader
import cypher.feature.parser.matchers.ResultWrapper
import cypher.feature.parser.{MatcherMatchingSupport, constructResultMatcher, parseParameters, statisticsParser}
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.{GraphDatabaseBuilder, GraphDatabaseFactory, GraphDatabaseSettings}
import org.neo4j.test.TestGraphDatabaseFactory
import org.opencypher.tools.tck.TCKCucumberTemplate
import org.opencypher.tools.tck.constants.TCKStepDefinitions._
import org.scalatest.{FunSuiteLike, Matchers}

import scala.util.{Failure, Success, Try}

class CypherTCKSteps extends FunSuiteLike with Matchers with TCKCucumberTemplate with MatcherMatchingSupport {

  val requiredScenarioName = FeatureSuiteTest.SCENARIO_NAME_REQUIRED.trim.toLowerCase

  val unsupportedScenarios = Set("Fail when adding new label predicate on already bound node 5")

  // Stateful
  var graph: GraphDatabaseService = null
  var result: Try[Result] = null
  var params: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
  var currentScenarioName: String = ""

  After() { _ =>
    ifEnabled {
      // TODO: postpone this till the last scenario
      graph.shutdown()
    }
  }

  Before() { scenario =>
    currentScenarioName = scenario.getName.toLowerCase
  }

  Background(BACKGROUND) {
    // do nothing, but necessary for the scala match
  }

  Given(NAMED_GRAPH) { (dbName: String) =>
    ifEnabled {
      val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DatabaseLoader(dbName))
      graph = loadConfig(builder).newGraphDatabase()
    }
  }

  Given(ANY_GRAPH) {
    ifEnabled {
      // We could do something fancy here, like randomising a state,
      // in order to guarantee that we aren't implicitly relying on an empty db.
      initEmpty()
    }
  }

  Given(EMPTY_GRAPH) {
    ifEnabled {
      initEmpty()
    }
  }

  And(INIT_QUERY) { (query: String) =>
    ifEnabled {
      // side effects are necessary for setting up graph state
      graph.execute(query)
    }
  }

  And(PARAMETERS) { (values: DataTable) =>
    ifEnabled {
      params = parseParameters(values)
    }
  }

  When(EXECUTING_QUERY) { (query: String) =>
    ifEnabled {
      result = Try {
        graph.execute(query, params)
      }
    }
  }

  Then(EXPECT_RESULT) { (expectedTable: DataTable) =>
    ifEnabled {
      val matcher = constructResultMatcher(expectedTable)

      inTx {
        matcher should accept(successful(result))
      }
    }
  }

  Then(EXPECT_ERROR) { (typ: String, phase: String, detail: String) =>
    ifEnabled {
      TCKErrorHandler(typ, phase, detail).check(result)
    }
  }

  Then(EXPECT_SORTED_RESULT) { (expectedTable: DataTable) =>
    ifEnabled {
      val matcher = constructResultMatcher(expectedTable)

      inTx {
        matcher should acceptOrdered(successful(result))
      }
    }
  }

  Then(EXPECT_EMPTY_RESULT) {
    ifEnabled {
      withClue("Expected empty result") {
        successful(result).hasNext shouldBe false
      }
    }
  }

  And(SIDE_EFFECTS) { (expectations: DataTable) =>
    ifEnabled {
      statisticsParser(expectations) should accept(successful(result).getQueryStatistics)
    }
  }

  And(NO_SIDE_EFFECTS) {
    ifEnabled {
      withClue("Expected no side effects") {
        successful(result).getQueryStatistics.containsUpdates() shouldBe false
      }
    }
  }

  private def ifEnabled(f: => Unit): Unit = {
    val blacklist = unsupportedScenarios.map(_.toLowerCase)
    if (!blacklist(currentScenarioName) && (requiredScenarioName.isEmpty || currentScenarioName.contains(requiredScenarioName))) {
      f
    }
  }

  When(EXECUTING_CONTROL_QUERY) { (query: String) =>
    result = Try {
      graph.execute(query, params)
    }
  }

  private def successful(value: Try[Result]): Result = value match {
    case Success(r) => new ResultWrapper(r)
    case Failure(e) => fail(s"Expected successful result, but got error: $e")
  }

  private def inTx(f: => Unit) = {
    val tx = graph.beginTx()
    f
    tx.success()
    tx.close()
  }

  private def initEmpty() =
    if (graph == null || !graph.isAvailable(1L)) {
      val builder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
      graph = loadConfig(builder).newGraphDatabase()
    }

  private def loadConfig(builder: GraphDatabaseBuilder): GraphDatabaseBuilder = {
    builder.setConfig(GraphDatabaseSettings.pagecache_memory, "8M")
    cypherConfig().map { case (s, v) => builder.setConfig(s, v) }
    builder
  }
}
