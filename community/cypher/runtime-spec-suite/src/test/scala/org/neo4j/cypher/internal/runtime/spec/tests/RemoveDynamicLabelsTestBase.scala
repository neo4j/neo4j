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
import org.neo4j.graphdb.Label
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException

import java.util

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class RemoveDynamicLabelsTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("remove single label") {
    givenGraph {
      nodeGraph(sizeHint, "RemoveMe")
    }

    removeLabelsTest(Seq("RemoveMe"), sizeHint)
  }

  test("remove multiple labels") {
    val nodeCount = 2
    val labels = Seq("RemoveMe", "MeToo", "AndMe")
    givenGraph {
      nodeGraph(2, labels.toSeq: _*)
    }

    removeLabelsTest(Seq("RemoveMe", "MeToo", "AndMe"), nodeCount * labels.size)
  }

  test("remove non-existing label") {
    val nodeCount = 10
    givenGraph {
      nodeGraph(nodeCount, "DontMindMe", "MeNeither")
    }

    removeLabelsTest(Seq("NonExisting", "Another"), 0)
  }

  test("remove subset of labels") {
    givenGraph {
      nodeGraph(7, "Cynist")
      nodeGraph(5, "Utilitarian")
      nodeGraph(3, "Nihilist")
      nodeGraph(2, "Cynist", "Nihilist")
    }

    removeLabelsTest(Seq("Cynist"), 7 + 2)
  }

  test("remove nodes on rhs of apply") {
    val nodeCount = 13
    givenGraph {
      nodeGraph(nodeCount, "DeleteMe", "DeleteMeToo")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.removeLabels("n", "DeleteMe", "DeleteMeToo")
      .|.argument("n")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("n")
      .withStatistics(labelsRemoved = 2 * nodeCount)
    val stream = tx.getAllNodes.stream()
    try {
      stream.filter(_.getLabels.iterator().asScala.nonEmpty).count() shouldBe 0
    } finally {
      stream.close()
    }
  }

  test("not return removed labels") {
    val nodeCount = 2
    val nodes = givenGraph {
      nodeGraph(2, "DeleteMe", "KeepMe")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "labels")
      .projection("n as n", "labels(n) as labels")
      .removeDynamicLabels("n", "'DeleteMe'")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = nodes.map(node => Array[Object](node, util.Arrays.asList("KeepMe")))

    runtimeResult should beColumns("n", "labels")
      .withRows(expected)
      .withStatistics(labelsRemoved = nodeCount)
  }

  test("should not remove too many labels if setLabel is between two loops with continuation") {
    val nodes = givenGraph {
      nodeGraph(sizeHint, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .removeDynamicLabels("n", "'Label'")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(n => Seq.fill(10)(n))))
      .withStatistics(labelsRemoved = sizeHint)
  }

  test("should fail if label to remove is null") {
    givenGraph {
      nodeGraph(sizeHint, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .removeDynamicLabels("n", "NULL")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] shouldBe thrownBy(consume(runtimeResult))
  }

  test("should fail if label on a node evaluates to an empty string") {
    // given a single node
    givenGraph {
      nodeGraph(sizeHint, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .removeDynamicLabels("n", "''")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    an[IllegalTokenNameException] shouldBe thrownBy(consume(runtimeResult))
  }

  test("should fail if label on a node evaluates to an invalid type") {
    // given a single node
    givenGraph {
      nodeGraph(sizeHint, "Label")
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .removeDynamicLabels("n", "1 + 2")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] shouldBe thrownBy(consume(runtimeResult))
  }

  test("should remove multiple dynamic labels from single list expression") {
    // given a single node
    givenGraph {
      nodeGraph(1, "L1", "L2", "L3", "L4")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .removeDynamicLabels("n", "['L1', 'L2', 'L3']")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labels = tx.getAllLabels.asScala.toSeq
    val labelsInUse = tx.getAllLabelsInUse.asScala.toSeq

    runtimeResult should beColumns("l").withSingleRow(util.Arrays.asList("L4")).withStatistics(labelsRemoved = 3)
    labels.map(_.name()) should equal(List("L1", "L2", "L3", "L4"))
    labelsInUse.map(_.name()) should equal(List("L4"))
  }

  test("should do nothing for an empty list") {
    // given a single node
    givenGraph {
      nodeGraph(1, "L1", "L2", "L3", "L4")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .removeDynamicLabels("n", "[]")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labels = tx.getAllLabels.asScala.toSeq
    val labelsInUse = tx.getAllLabelsInUse.asScala.toSeq

    runtimeResult should beColumns("l").withSingleRow(util.List.of("L1", "L2", "L3", "L4")).withStatistics(
      labelsRemoved = 0
    )
    labels.map(_.name()) should equal(List("L1", "L2", "L3", "L4"))
    labelsInUse.map(_.name()) should equal(List("L1", "L2", "L3", "L4"))
  }

  test("should fail if dynamic label expression contains a null") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .removeDynamicLabels("n", "['L1', NULL, 'L3']")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    a[CypherTypeException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should combine multiple dynamic label expressions") {
    // given a single node
    givenGraph {
      nodeGraph(1, "L1", "L2", "L3", "L4")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .removeDynamicLabels("n", "['L1', 'L2']", "'L3'", "['L4']")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labels = tx.getAllLabels.asScala.toSeq
    val labelsInUse = tx.getAllLabelsInUse.asScala.toSeq

    runtimeResult should beColumns("l").withSingleRow(util.Collections.emptyList()).withStatistics(labelsRemoved = 4)
    labels.map(_.name()) shouldBe List("L1", "L2", "L3", "L4")
    labelsInUse shouldBe empty
  }

  test("should combine multiple dynamic label expressions with static labels") {
    // given a single node
    givenGraph {
      nodeGraph(1, "L1", "L2", "L3", "L4", "L5")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .removeLabels("n", Seq("L1", "L2"), Seq("['L3', 'L4']", "'L5'"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labels = tx.getAllLabels.asScala.toSeq
    val labelsInUse = tx.getAllLabelsInUse.asScala.toSeq

    runtimeResult should beColumns("l").withSingleRow(util.Collections.emptyList()).withStatistics(labelsRemoved = 5)
    labels.map(_.name()) shouldBe List("L1", "L2", "L3", "L4", "L5")
    labelsInUse shouldBe empty
  }

  private def removeLabelsTest(removeLabels: Seq[String], expectedLabelsRemoved: Int): Unit = {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .removeDynamicLabels("n", removeLabels.map(l => s"'$l'"): _*)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("n")
      .withStatistics(labelsRemoved = expectedLabelsRemoved)
    removeLabels.foreach { label =>
      withClue(s"Number of nodes with label '$label' ") {
        Iterators.count(tx.findNodes(Label.label(label))) shouldBe 0
      }
    }
  }
}
