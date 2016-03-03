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
package cypher

import java.io.File
import java.nio.file.{Files, Path}
import java.util

import _root_.cucumber.api.DataTable
import _root_.cucumber.api.scala.{EN, ScalaDsl}
import cypher.GlueSteps._
import cypher.cucumber.DataTableConverter._
import cypher.cucumber.db.DatabaseConfigProvider.cypherConfig
import cypher.cucumber.db.DatabaseLoader
import cypher.feature.parser.{Accepters, constructResultMatcher}
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.{GraphDatabaseBuilder, GraphDatabaseFactory, GraphDatabaseSettings}
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.{FunSuiteLike, Matchers}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

class GlueSteps extends FunSuiteLike with Matchers with ScalaDsl with EN with Accepters {

  val Background = new Step("Background")

  var result: Result = null
  var graph: GraphDatabaseService = null

  private def initEmpty() =
    if (graph == null || !graph.isAvailable(1L)) {
      val builder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
      graph = loadConfig(builder).newGraphDatabase()
    }

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

  Given(USING_DB) { (dbName: String) =>
    val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DatabaseLoader(dbName))
    graph = loadConfig(builder).newGraphDatabase()
  }

  Given(ANY) {
    initEmpty()
  }

  Given(EMPTY) {
    initEmpty()
  }

  And(INIT_QUERY) { (query: String) =>
    // side effects are necessary for setting up graph state
    graph.execute(query)
  }

  When(EXECUTING_QUERY) { (query: String) =>
    result = graph.execute(query)
  }

  When(RUNNING_PARAMETRIZED_QUERY) { (query: String, params: DataTable) =>
    assert(!query.contains("cypher"), "init query should do specify pre parser options")
    val p = params.toList[AnyRef]
    assert(p.size == 1)
    result = graph.execute(query, castParameters(p.head))
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

  private def castParameters(map: java.util.Map[String, Object]) = {
    map.asScala.map { case (k, v) =>
      k -> Try(Integer.valueOf(v.toString)).getOrElse(v)
    }.asJava
  }

  private def loadConfig(builder: GraphDatabaseBuilder): GraphDatabaseBuilder = {
    val directory: Path = Files.createTempDirectory("tls")
    builder.setConfig(GraphDatabaseSettings.pagecache_memory, "8M")
    builder.setConfig(GraphDatabaseSettings.auth_store, new File(directory.toFile, "auth").getAbsolutePath)
    builder.setConfig("dbms.security.tls_key_file", new File(directory.toFile, "key.key").getAbsolutePath)
    builder.setConfig("dbms.security.tls_certificate_file", new File(directory.toFile, "cert.cert").getAbsolutePath)
    cypherConfig().map { case (s, v) => builder.setConfig(s, v) }
    builder
  }

  object sorter extends ((collection.Map[String, String], collection.Map[String, String]) => Boolean) {

    def apply(left: collection.Map[String, String], right: collection.Map[String, String]): Boolean = {
      val sortedKeys = left.keys.toList.sorted
      compareByKey(left, right, sortedKeys)
    }

    @tailrec
    private def compareByKey(left: collection.Map[String, String], right: collection.Map[String, String],
                             keys: collection.Seq[String]): Boolean = {
      if (keys.isEmpty)
        left.size < right.size
      else {
        val key = keys.head
        val l = left(key)
        val r = right(key)
        if (l == r)
          compareByKey(left, right, keys.tail)
        else l < r
      }
    }
  }

}

object GlueSteps {

  val USING_DB = """^using: (.*)$"""
  val RUNNING_PARAMETRIZED_QUERY = """^running parametrized: (.*)$"""

  // new constants:

  val BACKGROUND = "^$"

  // for Given
  val ANY = "^any graph$"
  val EMPTY = "^an empty graph$"

  // for And
  val INIT_QUERY = "^having executed: (.*)$"

  // for When
  val EXECUTING_QUERY = "^executing query: (.*)$"

  // for Then
  val EXPECT_RESULT = "^the result should be:$"
  val EXPECT_SORTED_RESULT = "^the result should be, in order:$"
  val EXPECT_EMPTY_RESULT = "^the result should be empty$"

}
