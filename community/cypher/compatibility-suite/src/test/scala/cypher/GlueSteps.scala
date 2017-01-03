/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package cypher

import java.nio.file.{Files, Path}
import java.util

import _root_.cucumber.api.DataTable
import _root_.cucumber.api.scala.{EN, ScalaDsl}
import cypher.GlueSteps._
import cypher.cucumber.db.DatabaseConfigProvider.cypherConfig
import cypher.cucumber.db.DatabaseLoader
import cypher.feature.parser.{Accepters, constructResultMatcher, parseParameters, statisticsParser}
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.{GraphDatabaseBuilder, GraphDatabaseSettings}
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.{FunSuiteLike, Matchers}

class GlueSteps extends FunSuiteLike with Matchers with ScalaDsl with EN with Accepters {

  val Background = new Step("Background")

  // Stateful
  var graph: GraphDatabaseService = null
  var result: Result = null
  var params: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()

  Before() { _ =>
    initEmpty()
  }

  After() { _ =>
    // TODO: postpone this till the last scenario
    graph.shutdown()
  }

  Background(BACKGROUND) {
    // do nothing, but necessary for the scala match
  }

  Given(NAMED_GRAPH) { (dbName: String) =>
    val builder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder(DatabaseLoader(dbName))
    graph = loadConfig(builder).newGraphDatabase()
  }

  Given(ANY) {
    // We could do something fancy here, like randomising a state,
    // in order to guarantee that we aren't implicitly relying on an empty db.
    initEmpty()
  }

  Given(EMPTY) {
    initEmpty()
  }

  And(INIT_QUERY) { (query: String) =>
    // side effects are necessary for setting up graph state
    graph.execute(query)
  }

  And(PARAMETERS) { (values: DataTable) =>
    params = parseParameters(values)
  }

  When(EXECUTING_QUERY) { (query: String) =>
    result = graph.execute(query, params)
  }

  Then(EXPECT_RESULT) { (expectedTable: DataTable) =>
    val matcher = constructResultMatcher(expectedTable)

    matcher should acceptResult(result)
  }

  Then(EXPECT_SORTED_RESULT) { (expectedTable: DataTable) =>
    val matcher = constructResultMatcher(expectedTable)

    matcher should acceptOrderedResult(result)
  }

  Then(EXPECT_EMPTY_RESULT) {
    result.hasNext shouldBe false
  }

  And(SIDE_EFFECTS) { (expectations: DataTable) =>
    statisticsParser(expectations) should acceptStatistics(result.getQueryStatistics)
  }

  private def initEmpty() =
    if (graph == null || !graph.isAvailable(1L)) {
      val builder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
      graph = loadConfig(builder).newGraphDatabase()
    }

  private def loadConfig(builder: GraphDatabaseBuilder): GraphDatabaseBuilder = {
    val directory: Path = Files.createTempDirectory("tls")
    builder.setConfig(GraphDatabaseSettings.pagecache_memory, "8M")
    cypherConfig().map { case (s, v) => builder.setConfig(s, v) }
    builder
  }

}

object GlueSteps {

  // for Background
  val BACKGROUND = "^$"

  // for Given
  val ANY = "^any graph$"
  val EMPTY = "^an empty graph$"
  val NAMED_GRAPH = """^the (.*) graph$"""

  // for And
  val INIT_QUERY = "^having executed: (.*)$"
  val PARAMETERS = "^parameters are:$"
  val SIDE_EFFECTS = "^the side effects should be:$"

  // for When
  val EXECUTING_QUERY = "^executing query: (.*)$"

  // for Then
  val EXPECT_RESULT = "^the result should be:$"
  val EXPECT_SORTED_RESULT = "^the result should be, in order:$"
  val EXPECT_EMPTY_RESULT = "^the result should be empty$"

}
