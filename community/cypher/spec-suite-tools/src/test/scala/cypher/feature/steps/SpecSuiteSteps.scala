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

import cucumber.api.DataTable
import cypher.SpecSuiteResources
import cypher.cucumber.db.DatabaseConfigProvider._
import cypher.cucumber.db.{GraphArchive, GraphArchiveImporter, GraphArchiveLibrary, GraphFileRepository}
import cypher.feature.parser.matchers.ResultWrapper
import cypher.feature.parser.{MatcherMatchingSupport, constructResultMatcher, parseParameters, statisticsParser}
import org.neo4j.graphdb.factory.{GraphDatabaseFactory, GraphDatabaseSettings}
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.test.TestGraphDatabaseFactory
import org.opencypher.tools.tck.TCKCucumberTemplate
import org.opencypher.tools.tck.constants.TCKStepDefinitions._
import org.scalatest.{FunSuiteLike, Matchers}

import scala.collection.JavaConverters._
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

trait SpecSuiteSteps extends FunSuiteLike with Matchers with TCKCucumberTemplate with MatcherMatchingSupport {

  // Implement in subclasses

  def specSuiteClass: Class[_]
  def unsupportedScenarios: Set[String]

  lazy val graphArchiveLibrary = new GraphArchiveLibrary(new GraphFileRepository(Path(SpecSuiteResources.targetDirectory(specSuiteClass, "graphs"))))
  lazy val requiredScenarioName = specSuiteClass.getField( "SCENARIO_NAME_REQUIRED" ).get( null ).toString.trim.toLowerCase

  // Stateful

  var graph: GraphDatabaseService = null
  var result: Try[Result] = null
  var params: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
  var currentScenarioName: String = ""

  // Steps

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
      lendForReadOnlyUse(dbName)
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

      val assertedSuccessful = successful(result)
      inTx {
        matcher should accept(assertedSuccessful)
      }
    }
  }

  Then(EXPECT_RESULT_UNORDERED_LISTS) { (expectedTable: DataTable) =>
    ifEnabled {
      val matcher = constructResultMatcher(expectedTable, unorderedLists = true)

      val assertedSuccessful = successful(result)
      inTx {
        matcher should accept(assertedSuccessful)
      }
    }
  }


  Then(EXPECT_ERROR) { (typ: String, phase: String, detail: String) =>
    ifEnabled {
      SpecSuiteErrorHandler(typ, phase, detail).check(result)
    }
  }

  Then(EXPECT_SORTED_RESULT) { (expectedTable: DataTable) =>
    ifEnabled {
      val matcher = constructResultMatcher(expectedTable)

      val assertedSuccessful = successful(result)
      inTx {
        matcher should acceptOrdered(assertedSuccessful)
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

  When(EXECUTING_CONTROL_QUERY) { (query: String) =>
    result = Try {
      graph.execute(query, params)
    }
  }

  private def ifEnabled(f: => Unit): Unit = {
    val blacklist = unsupportedScenarios.map(_.toLowerCase)
    if (!blacklist(currentScenarioName) && (requiredScenarioName.isEmpty || currentScenarioName.contains(requiredScenarioName))) {
      f
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
      builder.setConfig(currentDatabaseConfig("8M").asJava)
      graph = builder.newGraphDatabase()
    }

  private def lendForReadOnlyUse(recipeName: String) = {
    val recipe = graphArchiveLibrary.recipe(recipeName)
    val recommendedPcSize = recipe.recommendedPageCacheSize
    val pcSize = (recommendedPcSize/MB(32)+1)*MB(32)
    val config = currentDatabaseConfig(pcSize.toString)
    val archiveUse = GraphArchive(recipe, config).readOnlyUse
    val path = graphArchiveLibrary.lendForReadOnlyUse(archiveUse)(graphImporter)
    val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path.jfile)
    builder.setConfig(archiveUse.dbConfig.asJava)
    graph = builder.newGraphDatabase()
  }

  private def MB(v: Int) = v * 1024 * 1024

  private def currentDatabaseConfig(sizeHint: String) = {
    val builder = Map.newBuilder[String, String]
    builder += GraphDatabaseSettings.pagecache_memory.name() -> sizeHint
    cypherConfig().foreach { case (s, v) => builder += s.name() -> v }
    builder.result()
  }

  object graphImporter extends GraphArchiveImporter {
    protected def createDatabase(archive: GraphArchive.Descriptor, destination: Path): GraphDatabaseService = {
      val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(destination.jfile)
      builder.setConfig(archive.dbConfig.asJava)
      builder.newGraphDatabase()
    }
  }
}
