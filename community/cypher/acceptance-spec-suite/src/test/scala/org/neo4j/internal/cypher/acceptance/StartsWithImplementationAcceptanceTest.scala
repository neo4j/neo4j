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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_2.pipes.{IndexSeekByRange, UniqueIndexSeekByRange}
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}
import org.neo4j.graphdb.{Node, ResourceIterator}

class StartsWithImplementationAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  var aNode: Node = null
  var bNode: Node = null
  var cNode: Node = null
  var dNode: Node = null
  var eNode: Node = null
  var fNode: Node = null

  override def initTest() {
    super.initTest()
    aNode = createLabeledNode(Map("name" -> "ABCDEF"), "LABEL")
    bNode = createLabeledNode(Map("name" -> "AB"), "LABEL")
    cNode = createLabeledNode(Map("name" -> "abcdef"), "LABEL")
    dNode = createLabeledNode(Map("name" -> "ab"), "LABEL")
    eNode = createLabeledNode(Map("name" -> ""), "LABEL")
    fNode = createLabeledNode("LABEL")
  }

  test("should not plan an IndexSeek when index doesn't exist") {

    (1 to 10).foreach { _ =>
      createLabeledNode("Address")
    }

    createLabeledNode(Map("prop" -> "www123"), "Address")
    createLabeledNode(Map("prop" -> "www"), "Address")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:Address) WHERE a.prop STARTS WITH 'www' RETURN a")

    result should not(use(IndexSeekByRange.name))
  }

  test("Should handle prefix search with existing transaction state") {
    graph.createIndex("User", "name")
    graph.inTx {
      createLabeledNode(Map("name" -> "Stefan"), "User")
      createLabeledNode(Map("name" -> "Stephan"), "User")
      createLabeledNode(Map("name" -> "Stefanie"), "User")
      createLabeledNode(Map("name" -> "Craig"), "User")
    }
    graph.inTx {
      drain(graph.execute("MATCH (u:User {name: 'Craig'}) SET u.name = 'Steven'"))
      drain(graph.execute("MATCH (u:User {name: 'Stephan'}) DELETE u"))
      drain(graph.execute("MATCH (u:User {name: 'Stefanie'}) SET u.name = 'steffi'"))

      val result = executeWithAllPlannersAndCompatibilityMode("MATCH (u:User) WHERE u.name STARTS WITH 'Ste' RETURN u.name as name").columnAs("name").toList.toSet

      result should equal(Set[String]("Stefan", "Steven"))
    }
  }

  private def drain(iter: ResourceIterator[_]): Unit = {
    try {
      while (iter.hasNext) iter.next()
    } finally {
      iter.close()
    }
  }
}
