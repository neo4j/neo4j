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
    asScalaResult(result).toSet should equal(Set(Map("count(n.prop)" -> 4)))
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
    asScalaResult(result).toSet should equal(Set(Map("count(n.prop)" -> 2, "count(m.prop)" -> 2)))
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
    asScalaResult(result).toSet should equal(Set(
      Map("n.prop" -> "foo", "count(n.count)" -> 2),
      Map("n.prop" -> "bar", "count(n.count)" -> 1),
      Map("n.prop" -> null, "count(n.count)" -> 0)
      ))

    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support average") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN avg(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(Map("avg(n.prop)" -> 55.0)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support average with grouping") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i, "group" -> (if (i > 50) "FOO" else "BAR")))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.group, avg(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(
      Map("n.group" -> "FOO", "avg(n.prop)" -> 80.0),
      Map("n.group" -> "BAR", "avg(n.prop)" -> 30.0)
    ))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support max") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN max(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(Map("max(n.prop)" -> 100)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support max with grouping") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i, "group" -> (if (i > 50) "FOO" else "BAR")))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.group, max(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(
      Map("n.group" -> "FOO", "max(n.prop)" -> 100),
      Map("n.group" -> "BAR", "max(n.prop)" -> 50)
    ))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support min") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN min(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(Map("min(n.prop)" -> 10)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support min with grouping") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i, "group" -> (if (i > 50) "FOO" else "BAR")))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.group, min(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(
      Map("n.group" -> "FOO", "min(n.prop)" -> 60),
      Map("n.group" -> "BAR", "min(n.prop)" -> 10)
    ))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support collect") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN collect(n.prop)")

    //Then
    asScalaResult(result).toList.head("collect(n.prop)").asInstanceOf[Seq[_]] should contain theSameElementsAs List(10, 20, 30, 40, 50, 60,
                                                                                               70, 80, 90, 100)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support collect with grouping") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i, "group" -> (if (i > 50) "FOO" else "BAR")))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.group, collect(n.prop)")

    //Then
    val first :: second :: Nil = asScalaResult(result).toList
    first("n.group") should equal("FOO")
    first("collect(n.prop)").asInstanceOf[Seq[_]] should contain theSameElementsAs List(60, 70, 80, 90, 100)

    second("n.group") should equal("BAR")
    second("collect(n.prop)").asInstanceOf[Seq[_]] should contain theSameElementsAs List(10, 20, 30, 40, 50)

    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], String] = Map(GraphDatabaseSettings.cypher_morsel_size -> "4")
}
