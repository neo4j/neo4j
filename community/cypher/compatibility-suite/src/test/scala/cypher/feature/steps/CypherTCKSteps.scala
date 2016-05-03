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
import cypher.cucumber.db.DatabaseConfigProvider.cypherConfig
import cypher.cucumber.db.DatabaseLoader
import cypher.feature.parser.{MatcherMatchingSupport, constructResultMatcher, parseParameters, statisticsParser}
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.{GraphDatabaseBuilder, GraphDatabaseFactory, GraphDatabaseSettings}
import org.neo4j.test.TestGraphDatabaseFactory
import org.opencypher.tools.tck.TCKCucumberTemplate
import org.opencypher.tools.tck.constants.TCKErrorDetails._
import org.opencypher.tools.tck.constants.TCKStepDefinitions._
import org.opencypher.tools.tck.constants.{TCKErrorPhases, TCKErrorTypes}
import org.scalatest.{FunSuiteLike, Matchers}

import scala.util.{Failure, Success, Try}

class CypherTCKSteps extends FunSuiteLike with Matchers with TCKCucumberTemplate with MatcherMatchingSupport {

  // Stateful
  var graph: GraphDatabaseService = null
  var result: Try[Result] = null
  var params: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()

  After() { _ =>
    // TODO: postpone this till the last scenario
    graph.shutdown()
  }

  Background(BACKGROUND) {
    // do nothing, but necessary for the scala match
  }

  Given(NAMED_GRAPH) { (dbName: String) =>
    val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DatabaseLoader(dbName))
    graph = loadConfig(builder).newGraphDatabase()
  }

  Given(ANY_GRAPH) {
    // We could do something fancy here, like randomising a state,
    // in order to guarantee that we aren't implicitly relying on an empty db.
    initEmpty()
  }

  Given(EMPTY_GRAPH) {
    initEmpty()
  }

  And(INIT_QUERY) { (query: String) =>
    // side effects are necessary for setting up graph state
    graph.execute(query)
  }

  And(INIT_LONG_QUERY) { (query: String) =>
    // side effects are necessary for setting up graph state
    graph.execute(query)
  }

  And(PARAMETERS) { (values: DataTable) =>
    params = parseParameters(values)
  }

  When(EXECUTING_QUERY) { (query: String) =>
    result = Try {
      graph.execute(query, params)
    }
  }

  When(EXECUTING_LONG_QUERY) { (query: String) =>
    result = Try {
      graph.execute(query, params)
    }
  }

  Then(EXPECT_RESULT) { (expectedTable: DataTable) =>
    val matcher = constructResultMatcher(expectedTable)

    inTx {
      matcher should accept(successful(result))
    }
  }

  Then(EXPECT_ERROR) { (typ: String, phase: String, detail: String) =>
    phase match {
      case TCKErrorPhases.COMPILE_TIME => checkError(result, typ, phase, detail)
      case TCKErrorPhases.RUNTIME => result match {
        case Success(triedResult) =>
          // might need to exhaust result to provoke error
          val consumedResult = Try {
            while (triedResult.hasNext) {
              triedResult.next()
            }
            triedResult
          }
          checkError(consumedResult, typ, phase, detail)
        case x => checkError(x, typ, phase, detail)
      }
      case _ => fail(s"Unknown phase $phase specified. Supported values are '${TCKErrorPhases.COMPILE_TIME}' and '${TCKErrorPhases.RUNTIME}'.")
    }
  }

  private def checkError(result: Try[Result], typ: String, phase: String, detail: String) = {
    val statusType = if (typ == TCKErrorTypes.CONSTRAINT_VALIDATION_FAILED) "Schema" else "Statement"
    result match {
      case Failure(e: QueryExecutionException) =>
        s"Neo.ClientError.$statusType.$typ" should equal(e.getStatusCode)

        // Compile time errors
        if (e.getMessage.matches("Invalid input .+ is not a valid value, must be a positive integer[\\s.\\S]+"))
          detail should equal(NEGATIVE_INTEGER_ARGUMENT)
        else if (e.getMessage.matches("Can't use aggregate functions inside of aggregate functions\\."))
          detail should equal(NESTED_AGGREGATION)

        // Runtime errors
        else if (e.getMessage.matches("Expected .+ to be a java.lang.String, but it was a .+"))
          detail should equal(MAP_ELEMENT_ACCESS_BY_NON_STRING)
        else if (e.getMessage.matches("Expected .+ to be a java.lang.Number, but it was a .+"))
          detail should equal(LIST_ELEMENT_ACCESS_BY_NON_INTEGER)
        else if (e.getMessage.matches(".+ is not a collection or a map. Element access is only possible by performing a collection lookup using an integer index, or by performing a map lookup using a string key .+"))
          detail should equal(INVALID_ELEMENT_ACCESS)
        else if (e.getMessage.matches(".+ can not create a new node due to conflicts with( both)? existing( and missing)? unique nodes.*"))
          detail should equal(CREATE_BLOCKED_BY_CONSTRAINT)
        else if (e.getMessage.matches("Node [0-9]+ already exists with label .+ and property \".+\"=\\[.+\\]"))
          detail should equal(CREATE_BLOCKED_BY_CONSTRAINT)
        else if (e.getMessage.matches("Cannot delete node\\<\\d+\\>, because it still has relationships. To delete this node, you must first delete its relationships."))
          detail should equal(DELETE_CONNECTED_NODE)

        else fail(s"Unknown $phase error: $e", e)

      case Failure(e) =>
        fail(s"Unknown $phase error: $e", e)

      case _: Success[_] =>
        fail(s"No $phase error was raised")
    }
  }

  Then(EXPECT_SORTED_RESULT) { (expectedTable: DataTable) =>
    val matcher = constructResultMatcher(expectedTable)

    inTx {
      matcher should acceptOrdered(successful(result))
    }
  }

  Then(EXPECT_EMPTY_RESULT) {
    withClue("Expected empty result") {
      successful(result).hasNext shouldBe false
    }
  }

  And(SIDE_EFFECTS) { (expectations: DataTable) =>
    statisticsParser(expectations) should accept(successful(result).getQueryStatistics)
  }

  And(NO_SIDE_EFFECTS) {
    withClue("Expected no side effects") {
      successful(result).getQueryStatistics.containsUpdates() shouldBe false
    }
  }

  private def successful(value: Try[Result]): Result = value match {
    case Success(r) => r
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
