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
package org.neo4j.cypher.performance

import org.neo4j.cypher.ExecutionEngine
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{DynamicRelationshipType, GraphDatabaseService, Node}

import scala.util.Random

class PerformanceTest extends CypherFunSuite {
  val r = new Random()

  var db: GraphDatabaseService = null
  var engine: ExecutionEngine = null

  override def beforeEach() {
    super.beforeEach()
    db = new GraphDatabaseFactory().newEmbeddedDatabase("target/db");
    engine = new ExecutionEngine(db)
  }

  override def afterEach() {
    db.shutdown()
    super.afterEach()
  }

  ignore("createDatabase") {

    val startPoints = (0 to 10).map(x => {
      val tx = db.beginTx()

      val a = createNode()
      (0 to 10).foreach(y => {
        val b = createNode()
        relate(a, b)
        (0 to 50).foreach(z => {
          val c = createNode()
          val d = createNode()
          relate(b, c)
          relate(b, d)
        })
      })
      tx.success()
      tx.close()
      a
    })

    val engine = new ExecutionEngine(db)

    val t0 = System.nanoTime : Double
    engine.execute("start a=node({root}) match a-->b-->c, b-->d return a,count(*)", Map("root"->startPoints)).toList
    val t1 = System.nanoTime : Double
    println("Elapsed time " + (t1 - t0) / 1000000.0 + " msecs")
  }

  def createNode() = {
    db.createNode()
  }

  def relate(a: Node, b: Node) {
    a.createRelationshipTo(b, DynamicRelationshipType.withName("r"))
  }
}
