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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.delete
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setPropertyFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperty
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.internal.helpers.collection.Iterables

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class ForeachTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("foreach on empty input") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "[1, 2, 3]", Seq(createPattern(nodes = Seq(createNodeWithProperties("n", Seq("L"), "{prop: i}")))))
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x").withNoRows().withNoUpdates()
  }

  test("foreach with create pattern and one row in ") {
    // given
    val lhsRows = inputValues(Array(10))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "[1, 2, 3]", Seq(createPattern(nodes = Seq(createNodeWithProperties("n", Seq("L"), "{prop: i}")))))
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withSingleRow(10)
      .withStatistics(nodesCreated = 3, labelsAdded = 3, propertiesSet = 3)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.map(_.getProperty("prop")) should equal(List(1, 2, 3))
    } finally {
      allNodes.close()
    }
  }

  test("foreach on empty list") {
    // given
    val lhsRows = inputValues((1 to sizeHint).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "[]", Seq(createPattern(nodes = Seq(createNodeWithProperties("n", Seq("L"), "{prop: i}")))))
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to sizeHint))
      .withNoUpdates()
  }

  test("foreach with dependency on previous value") {
    // given
    val lhsRows = inputValues(Array(10), Array(20), Array(30))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach(
        "i",
        "[1, 2, 3]",
        Seq(createPattern(nodes = Seq(createNodeWithProperties("n", Seq("L"), "{prop: i + x}"))))
      )
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(Seq(10, 20, 30)))
      .withStatistics(nodesCreated = 9, labelsAdded = 9, propertiesSet = 9)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.map(_.getProperty("prop")) should contain theSameElementsAs List(11, 12, 13, 21, 22, 23, 31, 32,
        33)
    } finally {
      allNodes.close()
    }
  }

  test("foreach should handle many rows") {
    // given
    val size = sizeHint / 10
    val lhsRows: InputValues = inputValues((1 to size).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "[1, 2, 3]", Seq(createPattern(nodes = Seq(createNodeWithProperties("n", Seq("L"), "{prop: i}")))))
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to size))
      .withStatistics(nodesCreated = 3 * size, labelsAdded = 3 * size, propertiesSet = 3 * size)
  }

  test("foreach should work with eager") {
    // given
    givenGraph(nodeGraph(sizeHint))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("x.prop AS res")
      .eager()
      .foreach("n", "[x]", Seq(setNodeProperty("n", "prop", "42")))
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("res")
      .withRows(singleColumn(Seq.fill(sizeHint)(42)))
      .withStatistics(propertiesSet = sizeHint)
  }

  test("foreach on the rhs of an apply") {
    // given
    val size = sizeHint / 10
    val lhsRows: InputValues = inputValues((1 to size).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.foreach(
        "i",
        "[0, 1, 2]",
        Seq(createPattern(nodes = Seq(createNodeWithProperties("n", Seq("L"), "{prop: x + i}"))))
      )
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to size))
      .withStatistics(nodesCreated = 3 * size, labelsAdded = 3 * size, propertiesSet = 3 * size)
  }

  test("foreach where some lists are null") {
    // given
    val lhsRows = inputValues(
      Array(java.util.List.of(1, 2, 3)),
      Array(null),
      Array(java.util.List.of(1, 2)),
      Array(null),
      Array(java.util.List.of(1, 2, 3))
    )

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "x", Seq(createPattern(nodes = Seq(createNodeWithProperties("n", Seq("L"), "{prop: i}")))))
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(lhsRows.flatten)
      .withStatistics(nodesCreated = 8, labelsAdded = 8, propertiesSet = 8)
  }

  test("foreach should create nodes and relationships") {
    // given
    val size = sizeHint / 10
    val lhsRows: InputValues = inputValues((1 to size).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach(
        "i",
        "[1, 2, 3]",
        Seq(createPattern(
          nodes = Seq(createNode("n"), createNode("m")),
          relationships = Seq(createRelationship("r", "n", "R", "m"))
        ))
      )
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to size))
      .withStatistics(nodesCreated = 2 * 3 * size, relationshipsCreated = 3 * size)
  }

  test("foreach should set label") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("i", "[1, 2, 3]", Seq(setLabel("n", "A", "B", "C")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(labelsAdded = 3 * sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.hasLabel(Label.label("A")) shouldBe true
        n.hasLabel(Label.label("B")) shouldBe true
        n.hasLabel(Label.label("C")) shouldBe true
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach should + set label should handle null") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("node", "[n, null]", Seq(setLabel("node", "A", "B", "C")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(labelsAdded = 3 * sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.hasLabel(Label.label("A")) shouldBe true
        n.hasLabel(Label.label("B")) shouldBe true
        n.hasLabel(Label.label("C")) shouldBe true
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach should set node property") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("i", "[1, 2, 3]", Seq(setNodeProperty("n", "prop", "i")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(propertiesSet = 3 * sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.getProperty("prop") should equal(3)
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach + set node property should handle null") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("node", "[null, n]", Seq(setNodeProperty("node", "prop", "42")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(propertiesSet = sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.getProperty("prop") should equal(42)
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach should set node properties from map") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("i", "[1, 2, 3]", Seq(setNodePropertiesFromMap("n", "{prop : i}")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(propertiesSet = 3 * sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.getProperty("prop") should equal(3)
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach + set node properties from map should handle null") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("node", "[n, null]", Seq(setNodePropertiesFromMap("node", "{prop : 42}")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(propertiesSet = sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.getProperty("prop") should equal(42)
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach should set relationship property") {
    val (_, rels) = givenGraph(circleGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .foreach("i", "[1, 2, 3]", Seq(setRelationshipProperty("r", "prop", "i")))
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels))
      .withStatistics(propertiesSet = 3 * sizeHint)
    val allRelationships = tx.getAllRelationships
    try {
      allRelationships.asScala.foreach { r =>
        r.getProperty("prop") should equal(3)
      }
    } finally {
      allRelationships.close()
    }
  }

  test("foreach + set relationship property should handle null") {
    val (_, rels) = givenGraph(circleGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .foreach("rel", "[null, r]", Seq(setRelationshipProperty("rel", "prop", "42")))
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels))
      .withStatistics(propertiesSet = sizeHint)
    val allRelationships = tx.getAllRelationships
    try {
      allRelationships.asScala.foreach { r =>
        r.getProperty("prop") should equal(42)
      }
    } finally {
      allRelationships.close()
    }
  }

  test("foreach should set relationship properties from map") {
    val (_, rels) = givenGraph(circleGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .foreach("i", "[r]", Seq(setRelationshipPropertiesFromMap("i", "{prop : 42}")))
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels))
      .withStatistics(propertiesSet = sizeHint)
    val allRelationships = tx.getAllRelationships
    try {
      allRelationships.asScala.foreach { r =>
        r.getProperty("prop") should equal(42)
      }
    } finally {
      allRelationships.close()
    }
  }

  test("foreach + set relationship properties from map should handle null") {
    val (_, rels) = givenGraph(circleGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .foreach("i", "[r, null]", Seq(setRelationshipPropertiesFromMap("i", "{prop : 42}")))
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels))
      .withStatistics(propertiesSet = sizeHint)
    val allRelationships = tx.getAllRelationships
    try {
      allRelationships.asScala.foreach { r =>
        r.getProperty("prop") should equal(42)
      }
    } finally {
      allRelationships.close()
    }
  }

  test("foreach should set property") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("i", "[1, 2, 3]", Seq(setProperty("n", "prop", "i")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(propertiesSet = 3 * sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.getProperty("prop") should equal(3)
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach + set property should handle null") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("node", "[n, null]", Seq(setProperty("node", "prop", "42")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(propertiesSet = sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.getProperty("prop") should equal(42)
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach should set property from map") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("m", "[{prop1: 1}, {prop2: 2}, {prop3: 3}]", Seq(setPropertyFromMap("n", "m", removeOtherProps = false)))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(propertiesSet = 3 * sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.getProperty("prop1") should equal(1)
        n.getProperty("prop2") should equal(2)
        n.getProperty("prop3") should equal(3)
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach should set property from map + non fusable") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .foreach("m", "[{prop1: 1}, {prop2: 2}, {prop3: 3}]", Seq(setPropertyFromMap("n", "m", removeOtherProps = false)))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(propertiesSet = 3 * sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.getProperty("prop1") should equal(1)
        n.getProperty("prop2") should equal(2)
        n.getProperty("prop3") should equal(3)
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach + set property from map should handle null") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("node", "[n, null]", Seq(setPropertyFromMap("node", "{prop: 42}", removeOtherProps = false)))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(propertiesSet = sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.getProperty("prop") should equal(42)
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach + remove label") {
    val nodes = givenGraph(nodeGraph(sizeHint, "A", "B", "C"))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("node", "[n, null]", Seq(removeLabel("node", "A", "B")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(labelsRemoved = 2 * sizeHint)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.foreach { n =>
        n.hasLabel(Label.label("A")) shouldBe false
        n.hasLabel(Label.label("B")) shouldBe false
        n.hasLabel(Label.label("C")) shouldBe true
      }
    } finally {
      allNodes.close()
    }
  }

  test("foreach + delete") {
    val nodes = givenGraph(nodeGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("node", "[n, null]", Seq(delete("node")))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(nodesDeleted = sizeHint)
    Iterables.count(tx.getAllNodes) shouldBe 0
  }

  test("foreach + detach delete") {
    val (nodes, _) = givenGraph(circleGraph(sizeHint))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("node", "[n, null]", Seq(delete("node", forced = true)))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(nodesDeleted = sizeHint, relationshipsDeleted = sizeHint)
    Iterables.count(tx.getAllNodes) shouldBe 0
  }

  test("foreach should create nodes and ignore null properties") {
    // given
    val size = sizeHint / 10
    val lhsRows: InputValues = inputValues((1 to size).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach(
        "i",
        "[1, 2, 3]",
        Seq(createPattern(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{p1: 42, p2: null}"))))
      )
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to size))
      .withStatistics(nodesCreated = 3 * size, propertiesSet = 3 * size)
  }

  test("foreach should create relationships and ignore null properties") {
    // given
    val size = sizeHint / 10
    val nodes = givenGraph(nodeGraph(size))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach(
        "i",
        "[1, 2, 3]",
        Seq(createPattern(relationships =
          Seq(createRelationship("r", "x", "R", "x", properties = Some("{p1: 42, p2: null}")))
        ))
      )
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(nodes))
      .withStatistics(relationshipsCreated = 3 * size, propertiesSet = 3 * size)
  }
}
