/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

import scala.collection.Map

class MorselRuntimeAcceptanceTest extends ExecutionEngineFunSuite {

  test("should not use morsel by default") {
    //Given
    val result = graph.execute("MATCH (n) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should not equal "MORSEL"
  }

  test("should be able to ask for morsel") {
    //Given
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should fallback if morsel doesn't support query") {
    //Given
    val result = graph.execute("CYPHER runtime=morsel MATCH (n)-[*]->(m) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should not equal "MORSEL"
  }

  test("should warn that morsels are experimental") {
    //Given
    import scala.collection.JavaConverters._

    val result = graph.execute("CYPHER runtime=morsel EXPLAIN MATCH (n) RETURN n")

    // When (exhaust result)
    val notifications = result.getNotifications.asScala.toSet

    //Then
    notifications.head.getDescription should equal("You are using an experimental feature (use the morsel runtime at " +
                                                     "your own peril, not recommended to be run on production systems)")

  }

  test("should support count with no grouping") {
    //Given
    createNode("prop" -> "foo")
    createNode()
    createNode()
    createNode("prop" -> "foo")
    createNode("prop" -> "foo")
    createNode("prop" -> "foo")

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN count(n.prop)")

    //Then
    result.next().get("count(n.prop)") should equal(4)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support multiple counts with no grouping") {
    //Given
    relate(createNode("prop" -> "foo"),createNode())
    relate(createNode(), createNode("prop" -> "foo"))
    relate(createNode("prop" -> "foo"), createNode("prop" -> "foo"))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n)-->(m) RETURN count(n.prop), count(m.prop)")

    //Then
    val next = result.next()
    next.get("count(n.prop)") should equal(2)
    next.get("count(m.prop)") should equal(2)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support count with grouping") {
    //Given
    createNode("prop" -> "foo", "count" -> 1)
    createNode("prop" -> "foo", "count" -> 1)
    createNode("prop" -> "bar")
    createNode("prop" -> "bar", "count" -> 1)
    createNode()

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.prop,count(n.count)")

    //Then
    var next = result.next()
    next.get("n.prop") should equal("foo")
    next.get("count(n.count)") should equal(2)
    next = result.next()
    next.get("n.prop") shouldBe null
    next.get("count(n.count)") should equal(0)
    next = result.next()
    next.get("n.prop") should equal("bar")
    next.get("count(n.count)") should equal(1)

    result.hasNext shouldBe false
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], String] = Map(GraphDatabaseSettings.cypher_morsel_size -> "4")
}
