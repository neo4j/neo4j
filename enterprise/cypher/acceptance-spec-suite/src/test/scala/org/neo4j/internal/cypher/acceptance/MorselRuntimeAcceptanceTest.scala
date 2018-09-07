/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import java.util.concurrent.TimeUnit

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

import scala.collection.Map

abstract class MorselRuntimeAcceptanceTest extends ExecutionEngineFunSuite {

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

  test("should support top n < morsel size") {
    //Given
    1 to 100 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.prop ORDER BY n.prop DESC LIMIT 2")

    //Then
    val scalaresult = asScalaResult(result).toList
    scalaresult should equal(List(Map("n.prop" -> 100), Map("n.prop" -> 99)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support top n > morsel size") {
    //Given
    1 to 100 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.prop ORDER BY n.prop DESC LIMIT 6")

    //Then
    val scalaresult = asScalaResult(result).toList
    scalaresult should equal(List(Map("n.prop" -> 100), Map("n.prop" -> 99), Map("n.prop" -> 98),
      Map("n.prop" -> 97), Map("n.prop" -> 96), Map("n.prop" -> 95)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support collect with grouping") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i, "group" -> (if (i > 50) "FOO" else "BAR")))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.group, collect(n.prop)")

    //Then
    val first :: second :: Nil = asScalaResult(result).toList
    first("n.group") should equal("BAR")
    first("collect(n.prop)").asInstanceOf[Seq[_]] should contain theSameElementsAs List(10, 20, 30, 40, 50)

    second("n.group") should equal("FOO")
    second("collect(n.prop)").asInstanceOf[Seq[_]] should contain theSameElementsAs List(60, 70, 80, 90, 100)

    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support index scans") {
    // Given
    graph.createIndex("Person", "name")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => s"Satia$i")
    names.foreach(name => createLabeledNode(Map("name" -> name), "Person"))

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE EXISTS(n.name) RETURN n.name ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal(names.toSet)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support index seek") {
    // Given
    graph.createIndex("Person", "name")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => s"Satia$i")
    names.foreach(name => createLabeledNode(Map("name" -> name), "Person"))

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE n.name='Satia42' RETURN n.name ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal(Set("Satia42"))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support contains index seek") {
    // Given
    graph.createIndex("Person", "name")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => s"Satia$i")
    names.foreach(name => createLabeledNode(Map("name" -> name), "Person"))

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE n.name CONTAINS'tia4' RETURN n.name ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal(("Satia4" +: (0 to 9).map(i => s"Satia4$i")).toSet)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support composite indexes") {
    // Given
    graph.createIndex("Person", "name", "age")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => (i, s"Satia$i"))
    names.foreach {
      case (i,name) => createLabeledNode(Map("name" -> name, "age" -> i), "Person")
    }

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE n.name = 'Satia42' AND n.age = 42 RETURN n.name, n.age ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet should equal(Set(Map("n.name" -> "Satia42", "n.age" -> 42)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support range queries") {
    // Given
    graph.createIndex("Person", "age")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => (i, s"Satia$i"))
    names.foreach {
      case (i,name) => createLabeledNode(Map("name" -> name, "age" -> i), "Person")
    }

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE n.age < 42 RETURN n.name, n.age ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal((1 to 41).map(i => s"Satia$i").toSet)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support label scans") {
    // Given
    val names = (1 to 91).map(i => s"Satia$i")
    names.foreach(name => createLabeledNode(Map("name" -> name), "Person"))

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) RETURN n.name ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal(names.toSet)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("don't stall for nested plan expressions") {
    // Given
    graph.execute( """CREATE (a:A)
                     |CREATE (a)-[:T]->(:B),
                     |       (a)-[:T]->(:C)""".stripMargin)


    // When
    val result =
      graph.execute( """ CYPHER runtime=morsel
                       | MATCH (n)
                       | RETURN CASE
                       |          WHEN id(n) >= 0 THEN (n)-->()
                       |          ELSE 42
                       |        END AS p""".stripMargin)

    // Then
    //note that this query is currently not using morsel, however there was a bug leading to
    //a blocked threads for nested queries
   asScalaResult(result).toList should not be empty
  }

  test("aggregation should not overflow morsel") {
    // Given
    graph.execute( """
                     |CREATE (zadie: AUTHOR {name: "Zadie Smith"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "White teeth"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "The Autograph Man"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "On Beauty"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "NW"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "Swing Time"})""".stripMargin)

    // When
    val result = graph.execute("CYPHER runtime=morsel  MATCH (a)-[r]->(b) RETURN b.book as book, count(r), count(distinct a)")

    // Then
    asScalaResult(result).toList should not be empty
  }

  test("should not duplicate results in queries with multiple eager pipelines") {
    // Given
    graph.execute( """
                     |CREATE (zadie: AUTHOR {name: "Zadie Smith"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "White teeth", rating: 5})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "The Autograph Man", rating: 3})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "On Beauty", rating: 4})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "NW"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "Swing Time", rating: 5})""".stripMargin)

    // When
    val result = graph.execute("CYPHER runtime=morsel  MATCH (b:BOOK) RETURN b.book as book, count(b.rating) ORDER BY book")

    // Then
    asScalaResult(result).toList should have size 5
  }

  ignore("should support apply") {

    graph.createIndex("Person", "name")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))

    for(i <- 0 until 100) {
      createLabeledNode(Map("name" -> "me", "secondName" -> s"me$i"), "Person")
      createLabeledNode(Map("name" -> s"me$i", "secondName" -> "you"), "Person")
    }

    val query =
      """MATCH (p:Person { name:'me' })
        |MATCH (q:Person { name: p.secondName })
        |RETURN p, q""".stripMargin

    // When
    val result = graph.execute(s"CYPHER runtime=morsel $query")
    // Then
    val resultSet = asScalaResult(result).toSet
    println(result.getExecutionPlanDescription)
  }
}

class ParallelMorselRuntimeAcceptanceTest extends MorselRuntimeAcceptanceTest {
  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_morsel_size -> "4",
    GraphDatabaseSettings.cypher_worker_count -> "0"
  )
}

class SequentialMorselRuntimeAcceptanceTest extends MorselRuntimeAcceptanceTest {
  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_morsel_size -> "4",
    GraphDatabaseSettings.cypher_worker_count -> "1"
  )
}
