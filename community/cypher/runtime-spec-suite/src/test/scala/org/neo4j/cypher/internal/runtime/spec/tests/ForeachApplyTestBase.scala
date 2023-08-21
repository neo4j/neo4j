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
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class ForeachApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("foreachApply on empty lhs") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[1, 2, 3]")
      .|.create(createNodeWithProperties("n", Seq("L"), "{prop: i}"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x").withNoRows().withNoUpdates()
  }

  test("foreachApply should update for one incoming row") {
    // given
    val lhsRows = inputValues(Array(10))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[1, 2, 3]")
      .|.create(createNodeWithProperties("n", Seq("L"), "{prop: i}"))
      .|.argument("x")
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

  test("foreachApply together with anti-conditional apply") {
    // given
    val lhsRows = inputValues(Array(10))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[1, 2, 3]")
      .|.antiConditionalApply("y")
      .|.|.create(createNodeWithProperties("n", Seq("L"), "{prop: i}"))
      .|.|.argument("x")
      .|.optional()
      .|.filter("y.prop = 42")
      .|.allNodeScan("y")
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

  test("foreachApply on empty list") {
    // given
    val lhsRows = inputValues((1 to sizeHint).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[]")
      .|.create(createNodeWithProperties("n", Seq("L"), "{prop: i}"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to sizeHint))
      .withNoUpdates()
  }

  test("foreachApply with dependency on lhs") {
    // given
    val lhsRows = inputValues(Array(10), Array(20), Array(30))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[1, 2, 3]")
      .|.create(createNodeWithProperties("n", Seq("L"), "{prop: i + x}"))
      .|.argument("x")
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

  test("foreachApply should handle many rows") {
    // given
    val size = sizeHint / 10
    val lhsRows: InputValues = inputValues((1 to size).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[1, 2, 3]")
      .|.create(createNodeWithProperties("n", Seq("L"), "{prop: i}"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to size))
      .withStatistics(nodesCreated = 3 * size, labelsAdded = 3 * size, propertiesSet = 3 * size)
  }

  test("should work with eager") {
    // given
    val sizeHint = 5
    given(nodeGraph(sizeHint))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("x.prop AS res")
      .aggregation(
        Seq("x AS x"),
        Seq("count(x) AS xs")
      ) // TODO this should be eager once it has support in pipelined runtime
      .foreachApply("n", "[x]")
      .|.setProperty("n", "prop", "42")
      .|.argument("x")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("res")
      .withRows(singleColumn(Seq.fill(sizeHint)(42)))
      .withStatistics(propertiesSet = sizeHint)
  }

  test("nested foreachApply") {
    // given
    val lhsRows: InputValues = inputValues(Array(1))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[10, 100, 1000]")
      .|.foreachApply("j", "[0, 1, 2]")
      .|.|.create(createNodeWithProperties("n", Seq("L"), "{prop: x + i + j}"))
      .|.|.argument("x", "i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withSingleRow(1)
      .withStatistics(nodesCreated = 9, labelsAdded = 9, propertiesSet = 9)
    val allNodes = tx.getAllNodes
    try {
      allNodes.asScala.map(_.getProperty("prop")) should contain theSameElementsAs List(11, 12, 13, 101, 102, 103, 1001,
        1002, 1003)
    } finally {
      allNodes.close()
    }
  }

  test("handle deeply nested foreachApply") {
    // given
    val lhsRows: InputValues = inputValues(Array(1), Array(2), Array(3))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[1, 2]")
      .|.foreachApply("j", "[1, 2]")
      .|.|.foreachApply("k", "[1, 2]")
      .|.|.|.foreachApply("l", "[1, 2]")
      .|.|.|.|.foreachApply("m", "[1, 2]")
      .|.|.|.|.|.create(createNode("y"))
      .|.|.|.|.|.argument("x", "i", "j", "k", "l")
      .|.|.|.|.argument("x", "i", "j", "k")
      .|.|.|.argument("x", "i", "j")
      .|.|.argument("x", "i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(Seq(1, 2, 3)))
      .withStatistics(nodesCreated = 3 * 2 * 2 * 2 * 2 * 2)
  }

  test("foreach on the rhs of an apply") {
    // given
    val size = sizeHint / 10
    val lhsRows: InputValues = inputValues((1 to size).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.foreachApply("i", "[0, 1, 2]")
      .|.|.create(createNodeWithProperties("n", Seq("L"), "{prop: x + i}"))
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to size))
      .withStatistics(nodesCreated = 3 * size, labelsAdded = 3 * size, propertiesSet = 3 * size)
  }

  test("foreachApply where some lists are null") {
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
      .foreachApply("i", "x")
      .|.create(createNodeWithProperties("n", Seq("L"), "{prop: i}"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x")
      .withRows(lhsRows.flatten)
      .withStatistics(nodesCreated = 8, labelsAdded = 8, propertiesSet = 8)
  }

  test("foreachApply does coercion to list of single argument") {
    // given
    val lhsRows = inputValues(Array[Any](1))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "1")
      .|.create(createNode("n"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x").withSingleRow(1).withStatistics(nodesCreated = 1)
  }
}
