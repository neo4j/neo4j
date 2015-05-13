/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.bdd

import java.io.File

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}
import org.neo4j.cypher.cucumber.db.DatabaseLoader
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.helpers.collection.IteratorUtil

import scala.collection.JavaConverters._
import scala.collection.mutable

class GlueSteps extends CypherFunSuite with ScalaDsl with EN {

  var result: Result = null
  var graph: GraphDatabaseService = null

  Given("""^using: (.*)$""") { (dbName: String) =>
    val path: File = DatabaseLoader(dbName)
    graph = new GraphDatabaseFactory().newEmbeddedDatabase(path.getAbsolutePath)
  }

  When("""^running: (.*)$""") { (query: String) =>
    result = graph.execute(query)
  }

  Then("""^result:$""") { (names: DataTable) =>
    val expected: List[mutable.Map[String, AnyRef]] = names.asMaps(classOf[String], classOf[AnyRef]).asScala.toList.map(_.asScala)
    val actual: List[mutable.Map[String, AnyRef]] = IteratorUtil.asList(result).asScala.map(_.asScala).toList
    actual should equal(expected)
    result.close()

    // TODO: postpone this till the last scenario
    graph.shutdown()
  }
}
