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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.internal.helpers.collection.Iterators

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

abstract class CreateTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should create node with labels") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .create(createNode("n", "A", "B", "C"))
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult.awaitAll()
    val node = Iterables.single(tx.getAllNodes)
    runtimeResult should beColumns("n").withSingleRow(node).withStatistics(nodesCreated = 1, labelsAdded = 3)
    node.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C"))
  }

  test("should create node with labels and empty results") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .emptyResult()
      .create(createNode("n", "A", "B", "C"))
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val node = Iterables.single(tx.getAllNodes)
    runtimeResult should beColumns("n").withNoRows().withStatistics(nodesCreated = 1, labelsAdded = 3)
    node.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C"))
  }

  test("should create node with properties") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .create(createNodeWithProperties("n", Seq("A"), "{p1: 1, p2: 2, p3: 3}"))
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val node = Iterables.single(tx.getAllNodes)
    runtimeResult should beColumns("n").withSingleRow(node).withStatistics(
      nodesCreated = 1,
      labelsAdded = 1,
      propertiesSet = 3
    )
    node.getAllProperties.asScala should equal(Map("p1" -> 1, "p2" -> 2, "p3" -> 3))
  }

  test("should handle creating node with null properties") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .create(createNodeWithProperties("n", Seq("A"), "{p1: 1, p2: null}"))
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val node = Iterables.single(tx.getAllNodes)
    runtimeResult should beColumns("n").withSingleRow(node).withStatistics(
      nodesCreated = 1,
      labelsAdded = 1,
      propertiesSet = 1
    )
    node.getAllProperties.asScala should equal(Map("p1" -> 1))
  }

  test("should create two nodes with dependency") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("mprop", "nprop")
      .projection("m.prop AS mprop", "n.prop AS nprop")
      .create(
        createNodeWithProperties("m", Seq("A"), "{prop: 1}"),
        createNodeWithProperties("n", Seq("A"), "{prop: m.prop + 1}")
      )
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("mprop", "nprop").withSingleRow(1, 2).withStatistics(
      nodesCreated = 2,
      labelsAdded = 2,
      propertiesSet = 2
    )
  }

  test("should create many nodes with labels") {
    // given
    given {
      circleGraph(sizeHint, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .create(createNode("n", "A", "B", "C"))
      .expand("(x)--(y)")
      .nodeByLabelScan("x", "L", IndexOrderNone)
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val nodes = tx.findNodes(label("A")).asScala.toList
    nodes should have size 2 * sizeHint
    runtimeResult should beColumns("n").withRows(singleColumn(nodes)).withStatistics(
      nodesCreated = 2 * sizeHint,
      labelsAdded = 2 * 3 * sizeHint
    )
    nodes.foreach(n => n.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C")))
  }

  test("should not create node when LIMIT 0 before CREATE") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .create(createNode("n", "A", "B", "C"))
      .limit(0)
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    Iterables.count(tx.getAllNodes) shouldBe 0
    runtimeResult should beColumns("n").withNoRows().withNoUpdates()
  }

  test("should only create n nodes if LIMIT n") {
    // given
    given {
      circleGraph(sizeHint, "L")
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .create(createNode("n", "A", "B", "C"))
      .limit(3)
      .expand("(x)--(y)")
      .nodeByLabelScan("x", "L", IndexOrderNone)
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val nodes = tx.findNodes(label("A")).asScala.toList
    nodes should have size 3
    runtimeResult should beColumns("n").withRows(singleColumn(nodes)).withStatistics(nodesCreated = 3, labelsAdded = 9)
    nodes.foreach(n => n.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C")))
  }

  test("should handle LIMIT and CREATE on the RHS of an Apply") {
    // given
    given {
      circleGraph(sizeHint, "L")
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.create(createNode("n", "A", "B", "C"))
      .|.limit(1)
      .|.expand("(x)--(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "L", IndexOrderNone)
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val nodes = tx.findNodes(label("A")).asScala.toList
    nodes should have size sizeHint
    runtimeResult should beColumns("n").withRows(singleColumn(nodes)).withStatistics(
      nodesCreated = sizeHint,
      labelsAdded = 3 * sizeHint
    )
    nodes.foreach(n => n.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C")))
  }

  test("should create node with labels on the RHS of an Apply") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.create(createNode("n", "A", "B", "C"))
      .|.argument()
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val node = Iterables.single(tx.getAllNodes)
    runtimeResult should beColumns("n").withSingleRow(node).withStatistics(nodesCreated = 1, labelsAdded = 3)
    node.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C"))
  }

  test("should create many node with labels on the RHS of an Apply") {
    // given
    given {
      circleGraph(sizeHint, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.create(createNode("n", "A", "B", "C"))
      .|.expand("(x)--(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "L", IndexOrderNone)
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val nodes = tx.findNodes(label("A")).asScala.toList
    nodes should have size 2 * sizeHint
    runtimeResult should beColumns("n").withRows(singleColumn(nodes)).withStatistics(
      nodesCreated = 2 * sizeHint,
      labelsAdded = 2 * 3 * sizeHint
    )
    nodes.foreach(n => n.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C")))
  }

  test("should create relationship with type and properties") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .create(
        createNode("n", "A"),
        createNode("m", "B"),
        createRelationship("r", "n", "R", "m", OUTGOING, Some("{p1: 1, p2: 2, p3: 3}"))
      )
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val relationship = Iterables.single(tx.getAllRelationships)
    runtimeResult should beColumns("r").withSingleRow(relationship).withStatistics(
      nodesCreated = 2,
      labelsAdded = 2,
      relationshipsCreated = 1,
      propertiesSet = 3
    )
    relationship.getType.name() should equal("R")
    relationship.getAllProperties.asScala should equal(Map("p1" -> 1, "p2" -> 2, "p3" -> 3))
  }

  test("should create relationship with null property") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .create(
        createNode("n", "A"),
        createNode("m", "B"),
        createRelationship("r", "n", "R", "m", OUTGOING, Some("{p1: 1, p2: null}"))
      )
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val relationship = Iterables.single(tx.getAllRelationships)
    runtimeResult should beColumns("r").withSingleRow(relationship).withStatistics(
      nodesCreated = 2,
      labelsAdded = 2,
      relationshipsCreated = 1,
      propertiesSet = 1
    )
    relationship.getType.name() should equal("R")
    relationship.getAllProperties.asScala should equal(Map("p1" -> 1))
  }

  test("should create two relationships with dependency") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1prop", "r2prop")
      .projection("r1.prop AS r1prop", "r2.prop AS r2prop")
      .create(
        createNode("n", "A"),
        createNode("m", "B"),
        createRelationship("r1", "n", "R", "m", OUTGOING, Some("{prop: 1}")),
        createRelationship("r2", "n", "R", "m", OUTGOING, Some("{prop: r1.prop + 1}"))
      )
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r1prop", "r2prop").withSingleRow(1, 2).withStatistics(
      nodesCreated = 2,
      labelsAdded = 2,
      relationshipsCreated = 2,
      propertiesSet = 2
    )
  }

  test("should create many relationships on the RHS of an Apply") {
    // given
    given {
      circleGraph(sizeHint, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.create(createRelationship("r", "x", "NEW", "y", OUTGOING))
      .|.expand("(x)-[:R]-(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "L", IndexOrderNone)
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val allRelationships = tx.getAllRelationships
    try {
      val relationships = allRelationships.asScala.filter(_.getType.name() == "NEW").toList
      relationships should have size 2 * sizeHint
      runtimeResult should beColumns("r").withRows(singleColumn(relationships)).withStatistics(relationshipsCreated =
        2 * sizeHint
      )
    } finally {
      allRelationships.close()
    }
  }

  test("should fail to create relationship if both nodes are missing") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "n", OUTGOING))
      .input(nodes = Seq("n"))
      .build(readOnly = false)

    the[InternalException] thrownBy consume(
      execute(logicalQuery, runtime, inputValues(Array[Any](null)))
    ) should have message
      "Failed to create relationship `r`, node `n` is missing. If you prefer to simply ignore rows where a relationship node is missing, " +
      "set 'dbms.cypher.lenient_create_relationship = true' in neo4j.conf"
  }

  test("should fail to create relationship if start node is missing") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .create(createNode("m", "A"), createRelationship("r", "n", "R", "m", OUTGOING))
      .input(nodes = Seq("n"))
      .build(readOnly = false)

    the[InternalException] thrownBy consume(
      execute(logicalQuery, runtime, inputValues(Array[Any](null)))
    ) should have message
      "Failed to create relationship `r`, node `n` is missing. If you prefer to simply ignore rows where a relationship node is missing, " +
      "set 'dbms.cypher.lenient_create_relationship = true' in neo4j.conf"
  }

  test("should fail to create relationship if end node is missing") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .create(createNode("n", "A"), createRelationship("r", "n", "R", "m", OUTGOING))
      .input(nodes = Seq("m"))
      .build(readOnly = false)

    the[InternalException] thrownBy consume(
      execute(logicalQuery, runtime, inputValues(Array[Any](null)))
    ) should have message
      "Failed to create relationship `r`, node `m` is missing. If you prefer to simply ignore rows where a relationship node is missing, " +
      "set 'dbms.cypher.lenient_create_relationship = true' in neo4j.conf"
  }

  test("should create node with similarly named labels") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .create(createNode("n", "A", "B"), createNode("m", "AB"))
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val n = Iterators.single(tx.findNodes(label("A")))
    val m = Iterators.single(tx.findNodes(label("AB")))
    runtimeResult should beColumns("n", "m").withSingleRow(n, m).withStatistics(nodesCreated = 2, labelsAdded = 3)
  }

  test("should only create one node if create if followed by loop with continuation") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, $sizeHint) AS r")
      .create(createNode("n", "A", "B", "C"))
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val node = Iterables.single(tx.getAllNodes)
    runtimeResult should beColumns("r").withRows(singleColumn(1 to sizeHint)).withStatistics(
      nodesCreated = 1,
      labelsAdded = 3
    )
    node.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C"))
  }

  test("should not create too many nodes when create is after after loop with continuations") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .create(createNode("n", "A", "B", "C"))
      .unwind(s"range(1, $sizeHint) AS r")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val allNodes = tx.getAllNodes
    try {
      val nodes = allNodes.asScala
      runtimeResult should beColumns("r").withRows(singleColumn(1 to sizeHint)).withStatistics(
        nodesCreated = sizeHint,
        labelsAdded = 3 * sizeHint
      )
      nodes.foreach(n => n.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C")))
      nodes should have size sizeHint
    } finally {
      allNodes.close()
    }
  }

  test("should not create too many nodes when create is after after loop with continuations 2") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val nodes = given {
      nodeGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .create(createNode("m", "A", "B", "C"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
  }

  test("should not create too many nodes when create is after after loop with continuations 3") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val (nodes, _) = given {
      circleGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .create(createNode("o", "A", "B", "C"))
      .expand("(n)-->(m)")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes))
      .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
  }

  test("should only create two nodes and one relationships if create if followed by loop with continuation") {
    // given an empty data base
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, $size) AS r")
      .create(createNode("n"), createNode("m"), createRelationship("rel", "n", "R", "m"))
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(1 to size))
      .withStatistics(nodesCreated = 2, relationshipsCreated = 1)
  }

  test("should not create too many nodes if creates is between two loops with continuation") {
    // given an empty data base
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("n", "A", "B", "C"))
      .unwind(s"range(1, $size) AS r1")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)

    val allNodes = tx.getAllNodes
    try {
      val nodes = allNodes.asScala
      runtimeResult should beColumns("r1")
        .withRows(singleColumn((1 to size).flatMap(i => Seq.fill(10)(i))))
        .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
      nodes.foreach(n => n.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C")))
      nodes should have size size
    } finally {
      allNodes.close()
    }
  }

  test("should not create too many nodes if creates is between two loops with continuation 2") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val nodes = given {
      nodeGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("m", "A", "B", "C"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(n => Seq.fill(10)(n))))
      .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
  }

  test("should not create too many nodes if creates is between two loops with continuation 3") {
    // given an empty data base
    val size = 100

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("n", "A", "B", "C"))
      .input(variables = Seq("r1"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult =
      execute(logicalQuery, runtime, inputValues((1 to size).map(i => Array[Any](i)): _*))
    consume(runtimeResult)
    val allNodes = tx.getAllNodes
    try {
      val nodes = allNodes.asScala
      runtimeResult should beColumns("r1").withRows(singleColumn((1 to size).flatMap(i => Seq.fill(10)(i))))
        .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
      nodes.foreach(n => n.getLabels.asScala.map(_.name()).toList should equal(List("A", "B", "C")))
      nodes should have size size
    } finally {
      allNodes.close()
    }
  }

  test("should not create too many nodes if creates is between two loops with continuation 4") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val (_, rels) = given {
      circleGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .relationshipTypeScan("(n)-[r:R]-(m)")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels.flatMap(r => Seq.fill(2 * 10)(r))))
      .withStatistics(nodesCreated = 2 * size, labelsAdded = 2 * 3 * size)
  }

  test("should not create too many nodes after a single node by id seek") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val nodes = given {
      nodeGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .nodeByIdSeek("n", Set.empty, nodes.head.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(Seq.fill(10)(nodes.head)))
      .withStatistics(nodesCreated = 1, labelsAdded = 3)
  }

  test("should not create too many nodes after a multiple node by id seek") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val nodes = given {
      nodeGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .nodeByIdSeek("n", Set.empty, nodes.map(_.getId): _*)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(n => Seq.fill(10)(n))))
      .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
  }

  test("should not create too many nodes after a single directed relationship by id seek") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val (_, rels) = given {
      circleGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .directedRelationshipByIdSeek("r", "from", "to", Set.empty, rels.head.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(Seq.fill(10)(rels.head)))
      .withStatistics(nodesCreated = 1, labelsAdded = 3)
  }

  test("should not create too many nodes after a multiple directed relationship by id seek") {
    val size = 100
    val (_, rels) = given {
      // NOTE: using sizeHint here can make the tx state unnecessarily big
      circleGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .directedRelationshipByIdSeek("r", "from", "to", Set.empty, rels.map(_.getId): _*)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels.flatMap(r => Seq.fill(10)(r))))
      .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
  }

  test("should not create too many nodes after a single undirected relationship by id seek") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val (_, rels) = given {
      circleGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .undirectedRelationshipByIdSeek("r", "from", "to", Set.empty, rels.head.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(Seq.fill(20)(rels.head)))
      .withStatistics(nodesCreated = 2, labelsAdded = 6)
  }

  test("should not create too many nodes after a multiple undirected relationship by id seek") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val (_, rels) = given {
      circleGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .undirectedRelationshipByIdSeek("r", "from", "to", Set.empty, rels.map(_.getId): _*)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels.flatMap(r => Seq.fill(20)(r))))
      .withStatistics(nodesCreated = 2 * size, labelsAdded = 2 * 3 * size)
  }

  test("should not create too many nodes after a node index seek") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val nodes = given {
      nodeIndex("L", "prop")
      nodePropertyGraph(size, { case i => Map("prop" -> i) }, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .nodeIndexOperator("n:L(prop IN ???)", paramExpr = Some(listOf((0 until size).map(i => literalInt(i)): _*)))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(r => Seq.fill(10)(r))))
      .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
  }

  test("should not create too many nodes after a optional expand all") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val startNode = given {
      val Seq(start, end) = nodeGraph(2)
      (1 to size).foreach(_ => start.createRelationshipTo(end, RelationshipType.withName("R")))
      start
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .optionalExpandAll("(n)-->(m)")
      .nodeByIdSeek("n", Set.empty, startNode.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn((1 to size).flatMap(_ => Seq.fill(10)(startNode))))
      .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
  }

  test("should not create too many nodes after a optional expand into") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    val (startNode, endNode) = given {
      val Seq(start, end) = nodeGraph(2)
      (1 to size).foreach(_ => start.createRelationshipTo(end, RelationshipType.withName("R")))
      (start, end)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("o", "A", "B", "C"))
      .optionalExpandInto("(n)-->(m)")
      .cartesianProduct()
      .|.nodeByIdSeek("m", Set.empty, endNode.getId)
      .nodeByIdSeek("n", Set.empty, startNode.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn((1 to size).flatMap(_ => Seq.fill(10)(startNode))))
      .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
  }

  test("should handle create after eager followed by loop with continuation") {
    // given an empty data base
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("n"))
      .eager()
      .unwind(s"range(1, $sizeHint) AS r1")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r1")
      .withRows(singleColumn((1 to sizeHint).flatMap(i => Seq.fill(10)(i))))
      .withStatistics(nodesCreated = sizeHint)
  }

  test("should handle create after antiConditionalApply followed by loop with continuation") {
    // given an empty data base
    val size = 100
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("n"))
      .antiConditionalApply("r1")
      .|.argument()
      .unwind(s"range(1, $size) AS r1")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r1")
      .withRows(singleColumn((1 to size).flatMap(i => Seq.fill(10)(i))))
      .withStatistics(nodesCreated = size)
  }

  test("should handle create after union followed by loop with continuation") {
    // given an empty data base
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("n"))
      .union()
      .|.projection("'right' AS x")
      .|.argument()
      .projection("'left' AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x")
      .withRows(singleColumn(Seq.fill(10)("left") ++ Seq.fill(10)("right")))
      .withStatistics(nodesCreated = 2)
  }

  test("should handle create after union followed by loop with continuation2") {
    // given an empty data base
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .create(createNode("n"))
      .union()
      .|.unwind(s"range(1, $size) AS x")
      .|.argument()
      .unwind(s"range(1, $size) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x")
      .withRows(singleColumn((1 to size).flatMap(i => Seq.fill(20)(i))))
      .withStatistics(nodesCreated = 2 * size)
  }

  test("should create node when create precedes top 0") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .top(0, "n ASC")
      .create(createNode("n"))
      .argument()
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("n").withNoRows().withStatistics(nodesCreated = 1)
  }
}

abstract class LenientCreateRelationshipTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](
      edition.copyWith(
        GraphDatabaseSettings.cypher_lenient_create_relationship -> java.lang.Boolean.TRUE
      ),
      runtime
    ) {

  test("should ignore to create relationship if both nodes are missing") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "n", OUTGOING))
      .input(nodes = Seq("n"))
      .build(readOnly = false)

    val results = execute(logicalQuery, runtime, inputValues(Array[Any](null)))
    results should beColumns("r").withSingleRow(null).withNoUpdates()
  }

  test("should ignore to create relationship if start node is missing") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .create(createNode("m", "A"), createRelationship("r", "n", "R", "m", OUTGOING))
      .input(nodes = Seq("n"))
      .build(readOnly = false)

    val results = execute(logicalQuery, runtime, inputValues(Array[Any](null)))
    results should beColumns("r").withSingleRow(null).withStatistics(nodesCreated = 1, labelsAdded = 1)
  }

  test("should ignore to create relationship if end node is missing") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .create(createNode("n", "A"), createRelationship("r", "n", "R", "m", OUTGOING))
      .input(nodes = Seq("m"))
      .build(readOnly = false)

    val results = execute(logicalQuery, runtime, inputValues(Array[Any](null)))
    results should beColumns("r").withSingleRow(null).withStatistics(nodesCreated = 1, labelsAdded = 1)
  }
}
