/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class SetPropertiesFromMapRelationshipTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should add relationship property with removeOtherProps") {
    given {
      val nodes = nodeGraph(2)
      val relationship = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      relationship.setProperty("prop1", 1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.prop1 as p1", "r.prop2 as p2")
      .setPropertiesFromMap("r", "{prop2: 3}", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, 3).withStatistics(propertiesSet = 2)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should add relationship property without removeOtherProps") {
    given {
      val nodes = nodeGraph(2)
      val relationship = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      relationship.setProperty("prop1", 1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.prop1 as p1", "r.prop2 as p2")
      .setPropertiesFromMap("r", "{prop2: 3}", removeOtherProps = false)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(1, 3).withStatistics(propertiesSet = 1)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should remove all relationship properties with removeOtherProps") {
    given {
      val nodes = nodeGraph(2)
      val relationship = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      relationship.setProperty("prop1", 1)
      relationship.setProperty("prop2", 2)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.prop1 as p1", "r.prop2 as p2")
      .setPropertiesFromMap("r", "{prop1: null}", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, null).withStatistics(propertiesSet = 2)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should remove specific relationship property without removeOtherProps") {
    given {
      val nodes = nodeGraph(2)
      val relationship = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      relationship.setProperty("prop1", 1)
      relationship.setProperty("prop2", 2)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.prop1 as p1", "r.prop2 as p2")
      .setPropertiesFromMap("r", "{prop1: null}", removeOtherProps = false)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, 2).withStatistics(propertiesSet = 1)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should handle multiple set/remove without removeOtherProps") {
    given {
      val nodes = nodeGraph(2)
      val relationship = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      relationship.setProperty("prop1", 1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.prop1 as p1", "r.prop2 as p2")
      .setPropertiesFromMap("r", "{prop1: null}", removeOtherProps = false)
      .setPropertiesFromMap("r", "{prop2: r.prop1 + 1}", removeOtherProps = false)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, 2).withStatistics(propertiesSet = 2)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should handle multiple set/remove with removeOtherProps") {
    given {
      val nodes = nodeGraph(2)
      val relationship = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      relationship.setProperty("prop1", 1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.prop1 as p1", "r.prop2 as p2")
      .setPropertiesFromMap("r", "{prop1: 3}", removeOtherProps = true)
      .setPropertiesFromMap("r", "{prop2: r.prop1 + 1}", removeOtherProps = false)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(3, null)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should set and remove multiple relationship properties") {
    given {
      val nodes = nodeGraph(2)
      val relationship = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      relationship.setProperty("prop1", 1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2", "p3")
      .projection("r.prop1 as p1", "r.prop2 as p2", "r.propCopy as p3")
      .setPropertiesFromMap("r", "{propCopy: r.prop1, prop2: 2}", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2", "p3").withSingleRow(null, 2, 1).withStatistics(propertiesSet = 3)
    properties shouldBe Seq("prop1", "prop2", "propCopy")
  }

  test("should set relationship property to multiple relationships") {
    val (_, relationships) = given {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1")
      .projection("r.prop1 as p1")
      .setPropertiesFromMap("r", "{prop1: 1}", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p1")
      .withRows(singleColumn(relationships.map(_ => 1)))
      .withStatistics(propertiesSet = relationships.size)
    property shouldBe "prop1"
  }

  test("should set relationship property on rhs of apply") {
    val (_, relationships) = given {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1")
      .apply()
      .|.projection("r.prop1 as p1")
      .|.setPropertiesFromMap("r", "{prop1: 1}", removeOtherProps = true)
      .|.argument("r")
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p1")
      .withRows(singleColumn(relationships.map(_ => 1)))
      .withStatistics(propertiesSet = relationships.size)
    property shouldBe "prop1"
  }

  test("should set relationship property after limit") {
    given {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1")
      .projection("r.prop1 as p1")
      .setPropertiesFromMap("r", "{prop1: 1}", removeOtherProps = true)
      .limit(1)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p1")
      .withSingleRow(1)
      .withStatistics(propertiesSet = 1)
    property shouldBe "prop1"
  }

  test("should set same relationship property multiple times") {
    val (_, relationships) = given {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.prop1 as p1", "r.prop2 as p2")
      .setPropertiesFromMap("r", "{prop2: r.prop1 + 1}", removeOtherProps = true)
      .apply()
      .|.setPropertiesFromMap("r", "{prop1: 1}", removeOtherProps = true)
      .|.argument("r")
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2")
      .withRows(relationships.map(_ => Array[Any](null, 2)))
      .withStatistics(propertiesSet = relationships.size * 3)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should set cached relationship property") {
    given {
      val nodes = nodeGraph(2)
      val relationship = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      relationship.setProperty("prop1", 1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("cached")
      .projection("cacheR[r.prop1] as cached")
      .setPropertiesFromMap("r", "{prop1: 3}", removeOtherProps = true)
      .cacheProperties("r.prop1")
      .setPropertiesFromMap("r", "{prop1: 2}", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("cached")
      .withSingleRow(3)
      .withStatistics(propertiesSet = 2)
    property shouldBe "prop1"
  }

  test("should not add new token if relationship property is set to null value") {
    given {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1")
      .projection("r.prop1 as p1")
      .setPropertiesFromMap("r", "{prop1: null}", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p1")
      .withSingleRow(null)
      .withStatistics()
  }

  test("should throw on null map") {
    given {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "null", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    the[CypherTypeException] thrownBy consume(
      runtimeResult
    ) should have message "Expected Null() to be a map, but it was :`NO_VALUE`"
  }

  test("should handle empty map") {
    given {
      val nodes = nodeGraph(2)
      val r = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      r.setProperty("prop", "1")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "{}", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(null).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set relationship property on null node with removeOtherProps") {
    val r = given {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "{prop: 3}", removeOtherProps = true)
      .input(relationships = Seq("r"))
      .build(readOnly = false)

    val input = inputValues(Array(r), Array(null))

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime, input)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withRows(singleColumn(Seq(3, null))).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set relationship property on null node without removeOtherProps") {
    val r = given {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "{prop: 3}", removeOtherProps = false)
      .input(relationships = Seq("r"))
      .build(readOnly = false)

    val input = inputValues(Array(r), Array(null))

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime, input)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withRows(singleColumn(Seq(3, null))).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set relationship property from expression that requires null check") {
    val r = given {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "{prop: sin(null)}", removeOtherProps = true)
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
  }

  test("should count relationship updates even if values are not changed") {
    val r = given {
      val nodes = nodeGraph(2)
      val r = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      r.setProperty("prop", "100")
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "{prop: 100}", removeOtherProps = true)
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(100).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should fail when setting non-map relationship property") {
    given {
      val nodes = nodeGraph(2)
      val relationship = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      relationship.setProperty("prop1", 1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.prop1 as p1", "r.prop2 as p2")
      .setPropertiesFromMap("r", "3", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    val thrownException = the[CypherTypeException] thrownBy consume(runtimeResult)
    thrownException.getMessage should fullyMatch regex "Expected (.*)3(.*) to be a map, but it was :`Long\\(3\\)`".r
  }

  /*
   * virtual relationships
   */
  test("should delete existing properties from virtual relationship") {
    val relationships = given {
      val nodes = nodeGraph(3)
      val relationships = Seq(
        nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R")),
        nodes(1).createRelationshipTo(nodes(2), RelationshipType.withName("R"))
      )
      relationships.head.setProperty("prop1", 200)
      relationships.head.setProperty("prop2", 300)
      relationships(1).setProperty("prop1", 100)
      relationships(1).setProperty("prop3", 400)
      relationships
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2", "p3")
      .projection("r2.prop1 as p1", "r2.prop2 as p2", "r2.prop3 as p3")
      .setPropertiesFromMap("r2", "r1", removeOtherProps = true)
      .apply()
      .|.directedRelationshipByIdSeek("r2", "x2", "y2", Set.empty, relationships(1).getId)
      .directedRelationshipByIdSeek("r1", "x1", "y1", Set.empty, relationships.head.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2", "p3").withSingleRow(200, 300, null).withStatistics(propertiesSet = 3)
    properties shouldEqual Seq("prop1", "prop2", "prop3")
  }

  test("should update existing properties from virtual relationship") {
    val relationships = given {
      val nodes = nodeGraph(3)
      val relationships = Seq(
        nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R")),
        nodes(1).createRelationshipTo(nodes(2), RelationshipType.withName("R"))
      )
      relationships.head.setProperty("prop1", 200)
      relationships.head.setProperty("prop2", 300)
      relationships(1).setProperty("prop1", 100)
      relationships(1).setProperty("prop3", 400)
      relationships
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2", "p3")
      .projection("r2.prop1 as p1", "r2.prop2 as p2", "r2.prop3 as p3")
      .setPropertiesFromMap("r2", "r1", removeOtherProps = false)
      .apply()
      .|.directedRelationshipByIdSeek("r2", "x2", "y2", Set.empty, relationships(1).getId)
      .directedRelationshipByIdSeek("r1", "x1", "y1", Set.empty, relationships.head.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2", "p3").withSingleRow(200, 300, 400).withStatistics(propertiesSet = 2)
    properties shouldEqual Seq("prop1", "prop2", "prop3")
  }

  test("should update existing properties from virtual node to virtual relationship") {
    val relationships = given {
      val nodes = nodeGraph(2)
      val relationships = Seq(nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R")))
      nodes.head.setProperty("prop1", 200)
      nodes.head.setProperty("prop2", 300)
      relationships.head.setProperty("prop1", 100)
      relationships.head.setProperty("prop3", 400)
      relationships
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2", "p3")
      .projection("r.prop1 as p1", "r.prop2 as p2", "r.prop3 as p3")
      .setPropertiesFromMap("r", "x1", removeOtherProps = true)
      .directedRelationshipByIdSeek("r", "x1", "y1", Set.empty, relationships.head.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2", "p3").withSingleRow(200, 300, null).withStatistics(propertiesSet = 3)
    properties shouldEqual Seq("prop1", "prop2", "prop3")
  }

  test("should set relationship properties from map between two loops with continuation") {
    val rels = given {
      val (_, rs) = circleGraph(sizeHint)
      rs.foreach(_.setProperty("prop", 0))
      rs
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .setPropertiesFromMap("r", "{prop: r.prop + 1}", removeOtherProps = true)
      .relationshipTypeScan("(n)-[r:R]->(m)")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels.flatMap(r => Seq.fill(10)(r))))
      .withStatistics(propertiesSet = sizeHint)
    rels.map(_.getProperty("prop")).foreach(i => i should equal(1))
  }
}
