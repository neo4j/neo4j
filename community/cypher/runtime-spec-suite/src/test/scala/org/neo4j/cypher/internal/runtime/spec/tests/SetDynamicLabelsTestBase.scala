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
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.internal.helpers.collection.Iterables

import java.util

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

abstract class SetDynamicLabelsTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should set label on a node") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .setDynamicLabels("n", "'Label'")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labels = Iterables.single(tx.getAllLabels)
    val labelsInUse = Iterables.single(tx.getAllLabelsInUse)

    runtimeResult should beColumns("l").withSingleRow(util.Arrays.asList("Label")).withStatistics(labelsAdded = 1)
    labels.name() shouldBe "Label"
    labelsInUse.name() shouldBe "Label"
  }

  test("should fail if label on a node evaluates to NULL") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .setDynamicLabels("n", "NULL")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] shouldBe thrownBy(consume(runtimeResult))
  }

  test("should set the label on a node with the same label") {
    // given a single node with label
    givenGraph {
      nodeGraph(1, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .setDynamicLabels("n", "'Label'")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labels = Iterables.single(tx.getAllLabels)
    val labelsInUse = Iterables.single(tx.getAllLabelsInUse)

    // labelsAdded statistics should not change
    runtimeResult should beColumns("l").withSingleRow(util.Arrays.asList("Label")).withStatistics(labelsAdded = 0)
    labels.name() shouldBe "Label"
    labelsInUse.name() shouldBe "Label"
  }

  test("should set new label on a node with a label") {
    // given a single node with label
    givenGraph {
      nodeGraph(1, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .setDynamicLabels("n", "'New_Label'")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labels = Iterables.asList(tx.getAllLabels).asScala
    val labelsInUse = Iterables.asList(tx.getAllLabelsInUse).asScala

    runtimeResult should beColumns("l")
      .withRows(Seq(Array(List("New_Label", "Label").asJava)), true)
      .withStatistics(labelsAdded = 1)

    labels.map(_.name()) should contain theSameElementsAs Vector("Label", "New_Label")
    labelsInUse.map(_.name()) should contain theSameElementsAs Vector("Label", "New_Label")
  }

  test("should throw on set label to node property") {
    // given a single node
    givenGraph {
      nodePropertyGraph(1, { case i => Map("prop" -> "1") })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1")
      .setDynamicLabels("p1", "'Label'")
      .projection("n.prop as p1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    val exception = the[Neo4jException] thrownBy {
      consume(runtimeResult)
    }

    exception match {
      case _: CypherTypeException         => succeed
      case _: ParameterWrongTypeException => succeed
      case e => fail(
          s"Expected exception CypherTypeException or ParameterWrongTypeException to be thrown, but ${e.getClass.getSimpleName} was thrown"
        )
    }
  }

  test("should not throw on set label to null node") {
    // given a null node
    val input = inputValues(Array[Any](null))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .setDynamicLabels("x", "'Label'")
      .input(nodes = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime, input)
    consume(runtimeResult)
    val labels = Iterables.asList(tx.getAllLabels).asScala
    val labelsInUse = Iterables.asList(tx.getAllLabelsInUse).asScala

    runtimeResult should beColumns("x").withSingleRow(null).withStatistics(labelsAdded = 0)

    labels.map(_.name()) shouldBe empty
    labelsInUse.map(_.name()) shouldBe empty
  }

  test("should set label on a node and remove it") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val setLabelsLogicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .setDynamicLabels("n", "'Label'")
      .allNodeScan("n")
      .build(readOnly = false)
    val setLabelsRuntimeResult: RecordingRuntimeResult = execute(setLabelsLogicalQuery, runtime)
    consume(setLabelsRuntimeResult)

    val removeLabelsLogicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .removeDynamicLabels("n", "'Label'")
      .allNodeScan("n")
      .build(readOnly = false)

    val removeLabelsRuntimeResult: RecordingRuntimeResult = execute(removeLabelsLogicalQuery, runtime)
    consume(removeLabelsRuntimeResult)

    val labels = Iterables.single(tx.getAllLabels)
    val labelsInUse = tx.getAllLabelsInUse

    removeLabelsRuntimeResult should beColumns("l").withSingleRow(util.Arrays.asList())
      .withStatistics(labelsRemoved = 1)
    labels.name() shouldBe "Label"
    labelsInUse.asScala shouldBe empty
  }

  test("should lock node") {
    // given a single node
    val n = givenGraph {
      nodeGraph(1).head
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .setDynamicLabels("n", "'Label'")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labels = Iterables.single(tx.getAllLabels)
    val labelsInUse = Iterables.single(tx.getAllLabelsInUse)

    runtimeResult should beColumns("l")
      .withSingleRow(util.Arrays.asList("Label"))
      .withStatistics(labelsAdded = 1)
      .withLockedNodes(Set(n.getId))

    labels.name() shouldBe "Label"
    labelsInUse.name() shouldBe "Label"

  }

  test("should set label from refslot") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(nRef) AS l")
      .setDynamicLabels("nRef", "'Label'")
      .unwind("[n] as nRef")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labels = Iterables.single(tx.getAllLabels)
    val labelsInUse = Iterables.single(tx.getAllLabelsInUse)

    runtimeResult should beColumns("l").withSingleRow(util.Arrays.asList("Label")).withStatistics(labelsAdded = 1)
    labels.name() shouldBe "Label"
    labelsInUse.name() shouldBe "Label"
  }

  test("should set multiple labels from refslot/longslot") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("refSlotLabels", "longSlotLabels")
      .projection("labels(nRef) AS refSlotLabels", "labels(n) AS longSlotLabels")
      .setDynamicLabels("n", "'LongSlotLabel'", "'CommonLabel'")
      .setDynamicLabels("nRef", "'RefSlotLabel", "'CommonLabel'")
      .unwind("[n] as nRef")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labelNames = Iterables.asList(tx.getAllLabels).asScala.toSeq.map(_.name())
    val labelNamesInUse = Iterables.asList(tx.getAllLabelsInUse).asScala.toSeq.map(_.name())

    runtimeResult should beColumns("refSlotLabels", "longSlotLabels")
      .withRows(
        Seq(Array(
          util.Arrays.asList("LongSlotLabel", "RefSlotLabel", "CommonLabel"),
          util.Arrays.asList("LongSlotLabel", "RefSlotLabel", "CommonLabel")
        )),
        listInAnyOrder = true
      )
      .withStatistics(labelsAdded = 3)
    labelNames should contain theSameElementsAs Seq("RefSlotLabel", "LongSlotLabel", "CommonLabel")
    labelNamesInUse should contain theSameElementsAs labelNames
  }

  test("should set label on multiple nodes") {
    // given a single node
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .setLabels("n", Seq("Label"), Seq("'OtherLabel'"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labelNames = Iterables.asList(tx.getAllLabels).asScala.toSeq.map(_.name())
    val labelNamesInUse = Iterables.asList(tx.getAllLabels).asScala.toSeq.map(_.name())

    val expectedRows = Seq.fill(sizeHint)(Array(util.Arrays.asList("Label", "OtherLabel")))
    runtimeResult should beColumns("l")
      .withRows(expectedRows, listInAnyOrder = true)
      .withStatistics(labelsAdded = 2 * sizeHint)

    labelNames should contain theSameElementsAs labelNamesInUse
    labelNames should contain theSameElementsAs Seq("Label", "OtherLabel")
  }

  test("should set label on multiple nodes with limit") {
    val limit = 2
    // given a single node
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .exhaustiveLimit(limit)
      .setLabels("n", Seq("Label"), Seq("'OtherLabel'"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labelNames = Iterables.asList(tx.getAllLabels).asScala.toSeq.map(_.name())
    val labelNamesInUse = Iterables.asList(tx.getAllLabels).asScala.toSeq.map(_.name())

    val expectedRows = Seq.fill(limit)(Array(util.Arrays.asList("Label", "OtherLabel")))
    runtimeResult should beColumns("l")
      .withRows(expectedRows, listInAnyOrder = true)
      .withStatistics(labelsAdded = 2 * sizeHint)

    labelNames should contain theSameElementsAs labelNamesInUse
    labelNames should contain theSameElementsAs Seq("Label", "OtherLabel")
  }

  test("should set label on multiple nodes with limit on top of apply") {
    val limit = 2
    // given a single node
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .exhaustiveLimit(limit)
      .apply()
      .|.projection("labels(n) AS l")
      .|.setLabels("n", Seq("Label"), Seq("'OtherLabel'"))
      .|.argument()
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val labelNames = Iterables.asList(tx.getAllLabels).asScala.toSeq.map(_.name())
    val labelNamesInUse = Iterables.asList(tx.getAllLabels).asScala.toSeq.map(_.name())

    val expectedRows = Seq.fill(limit)(Array(util.Arrays.asList("Label", "OtherLabel")))
    runtimeResult should beColumns("l")
      .withRows(expectedRows, listInAnyOrder = true)
      .withStatistics(labelsAdded = 2 * sizeHint)

    labelNames should contain theSameElementsAs labelNamesInUse
    labelNames should contain theSameElementsAs Seq("Label", "OtherLabel")
  }

  test("should set label then remove it") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .eager()
      .removeDynamicLabels("n", "'Label'")
      .setLabels("n", "Label")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)

    val labelNames = Iterables.asList(tx.getAllLabels).asScala.toSeq.map(_.name())
    val labelNamesInUse = Iterables.asList(tx.getAllLabels).asScala.toSeq.map(_.name())

    val expectedRows = Seq(Array(util.Arrays.asList()))
    runtimeResult should beColumns("l")
      .withRows(expectedRows, listInAnyOrder = true)
      .withStatistics(labelsAdded = 1, labelsRemoved = 1)

    labelNames should contain theSameElementsAs labelNamesInUse
    labelNames should contain theSameElementsAs Seq("Label")
  }

  test("should not create too many labels if setLabel is between two loops with continuation") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .setDynamicLabels("n", "'Label'")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(n => Seq.fill(10)(n))))
      .withStatistics(labelsAdded = sizeHint)
  }
}
