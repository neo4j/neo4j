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
import cypher.cucumber.prettifier.{makeTxSafe, prettifier}
import cypher.feature.parser.parseFullTable
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.{GraphDatabaseBuilder, GraphDatabaseFactory, GraphDatabaseSettings}
import org.neo4j.helpers.collection.IteratorUtil
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FunSuiteLike, Matchers}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class GlueSteps extends FunSuiteLike with Matchers with ScalaDsl with EN {

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
    val expectedRows = parseFullTable(expectedTable)
    val actualRows = makeTxSafe(new GraphDatabaseCypherService(graph), result)

    expectedRows should represent(actualRows)
  }

  Then(RESULT) { (sorted: Boolean, names: DataTable) =>
    val expected = names.asScala[String]
    val actual = IteratorUtil.asList(result).asScala.map(_.asScala).toList.map {
      _.map { case (k, v) => (k, prettifier.prettify(graph, v)) }
    }

    if (sorted) {
      actual should equal(expected)
    } else {
      // if it is not sorted let's sort the result before checking equality
      actual.sortWith(sorter) should equal(expected.sortWith(sorter))
    }

    result.close()
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
  val RESULT = """^(sorted )?result:$"""

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

}


case class represent(expected: util.List[util.Map[String, AnyRef]])
  extends Matcher[util.List[util.Map[String, AnyRef]]] {

  override def apply(actual: util.List[util.Map[String, AnyRef]]): MatchResult = {
    MatchResult(matches = compare(expected, actual), "a mismatch found", "no mismatches found")
  }


  def compare(expected: util.List[util.Map[String, AnyRef]], actual: util.List[util.Map[String, AnyRef]]): Boolean = {
    val expSorted = expected.asScala.toVector.map(_.asScala).sortBy(_.hashCode())
    val actSorted = actual.asScala.toVector.map(_.asScala).sortBy(_.hashCode())

    val bools = expSorted.zipWithIndex.map { case (expMap, index) =>
      val b = cypherEqual(expSorted(index), actSorted(index))
      if (!b)
        println(s"not equal: $expMap and ${actSorted(index)}")
      b
    }
    bools.reduce(_ && _)
  }


  def cypherEqual(expected: mutable.Map[String, AnyRef], actual: mutable.Map[String, AnyRef]) = {
    val keys = expected.keySet == actual.keySet
    val map = expected.keySet.map { key =>
      cypherEqual2(expected(key), actual(key))
    }
    keys && map.reduce(_ && _)
  }

  def nodeEquals(expected: Node, actual: Node): Boolean = {
    val labels = cypherEqual2(expected.getLabels, actual.getLabels)
    val properties = cypherEqual2(expected.getAllProperties, actual.getAllProperties)

    labels && properties
  }

  def relEquals(expected: Relationship, actual: Relationship): Boolean = ???

  def pathEquals(expected: Path, actual: Path): Boolean = ???

  def listEquals(expected: Seq[AnyRef], actual: Seq[AnyRef]): Boolean = ???

  def mapEquals(expected: Map[String, AnyRef], actual: Map[String, AnyRef]): Boolean = ???

  def cypherEqual2(expected: AnyRef, actual: AnyRef): Boolean = expected match {
    case null => actual == null
    case n: Node => nodeEquals(n, actual.asInstanceOf[Node])
    case r: Relationship => relEquals(r, actual.asInstanceOf[Relationship])
    case p: Path => pathEquals(p, actual.asInstanceOf[Path])
    case l: Seq[_] => listEquals(l.asInstanceOf[Seq[AnyRef]], actual.asInstanceOf[Seq[AnyRef]])
    case m: Map[_, _] => mapEquals(m.asInstanceOf[Map[String, AnyRef]], actual.asInstanceOf[Map[String, AnyRef]])
    case _ => expected == actual
  }

}
