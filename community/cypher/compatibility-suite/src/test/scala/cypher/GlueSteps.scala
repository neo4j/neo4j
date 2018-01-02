/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import _root_.cucumber.api.DataTable
import _root_.cucumber.api.scala.{EN, ScalaDsl}
import cypher.cucumber.db.DatabaseConfigProvider.cypherConfig
import cypher.cucumber.db.DatabaseLoader
import cypher.cucumber.prettifier.prettifier
import org.neo4j.graphdb.factory.{GraphDatabaseBuilder, GraphDatabaseFactory, GraphDatabaseSettings}
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.helpers.collection.IteratorUtil
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.{FunSuiteLike, Matchers}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

class GlueSteps extends FunSuiteLike with Matchers with ScalaDsl with EN {

  import cypher.GlueSteps._
  import cypher.cucumber.DataTableConverter._

  var result: Result = null
  var graph: GraphDatabaseService = null

  After() { _ =>
    // TODO: postpone this till the last scenario
    graph.shutdown()
  }

  Given(USING_DB) { (dbName: String) =>
    val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DatabaseLoader(dbName))
    graph = loadConfig(builder).newGraphDatabase()
  }

  Given(INIT_DB) { (initQuery: String) =>
    val builder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
    graph = loadConfig(builder).newGraphDatabase()
    assert(!initQuery.contains("cypher"), "init query should do specify pre parser options")
    // init update queries should go with rule+interpreted regardless the database configuration
    graph.execute(s"cypher planner=rule runtime=interpreted $initQuery")
  }

  When(RUNNING_QUERY) { (query: String) =>
    assert(!query.contains("cypher"), "init query should do specify pre parser options")
    result = graph.execute(query)
  }

  When(RUNNING_PARAMETRIZED_QUERY) { (query: String, params: DataTable) =>
    assert(!query.contains("cypher"), "init query should do specify pre parser options")
    val p = params.toList[AnyRef]
    assert(p.size == 1)
    result = graph.execute(query, castParameters(p.head))
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
    builder.setConfig(GraphDatabaseSettings.pagecache_memory, "8M")
    cypherConfig().map { case (s, v) => builder.setConfig(s, v) }
    builder
  }

  object sorter extends ((collection.Map[String, String], collection.Map[String, String]) => Boolean) {
    def apply(left: collection.Map[String, String], right: collection.Map[String, String]): Boolean = {
      val sortedKeys = left.keys.toList.sorted
      compareByKey(left, right, sortedKeys)
    }

    @tailrec
    private def compareByKey(left: collection.Map[String, String], right: collection.Map[String, String], keys: collection.Seq[String]): Boolean = {
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
  val INIT_DB = """^init: (.*)$"""
  val USING_DB = """^using: (.*)$"""
  val RUNNING_QUERY = """^running: (.*)$"""
  val RUNNING_PARAMETRIZED_QUERY = """^running parametrized: (.*)$"""
  val RESULT = """^(sorted )?result:$"""
}
