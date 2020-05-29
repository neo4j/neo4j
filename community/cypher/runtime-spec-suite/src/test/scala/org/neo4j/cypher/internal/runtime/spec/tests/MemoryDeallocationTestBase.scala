/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.io.ByteUnit
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

import scala.util.Random

object MemoryDeallocationTestBase {
  // The configured max memory per transaction in Bytes
  val maxMemory: Long = ByteUnit.mebiBytes(3)
}

abstract class MemoryDeallocationTestBase[CONTEXT <: RuntimeContext](
                                                                    edition: Edition[CONTEXT],
                                                                    runtime: CypherRuntime[CONTEXT],
                                                                    val sizeHint: Int
                                                                  )
  extends RuntimeTestSuite[CONTEXT](edition.copyWith(
    GraphDatabaseSettings.track_query_allocation -> java.lang.Boolean.TRUE,
    GraphDatabaseSettings.memory_transaction_max_size -> Long.box(MemoryManagementTestBase.maxMemory)), runtime) with InputStreams[CONTEXT] {

  private val sizeHintToUse = sizeHint

  // TODO: FIXME Pipelined LHSAccumulatingRHSStreamingSource has a reference counting bug with empty continuations that
  //             causes this test to fail for certain morsel sizes.
  //             We work around this by avoiding such cases to get some test coverage anyway.
  private def sizeHintToUseWithWorkaroundForPipelined: Int = {
    if (runtime.name.toLowerCase == "pipelined" && sizeHintToUse % edition.runtimeConfig().pipelinedBatchSizeBig == 0) {
      assume(edition.runtimeConfig().pipelinedBatchSizeBig > 1) // If morsel size is 1 this workaround will not work, so just skip it for this test run
      sizeHintToUse + 1
    } else {
      sizeHintToUse
    }
  }

  test("should deallocate memory between grouping aggregation - many groups") {
    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val nRows = sizeHintToUse

    // then
    compareMemoryUsageWithInputRows(logicalQuery1, logicalQuery2, nRows)
  }

  test("should deallocate memory between grouping aggregation - one large group") {
    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("d")
      .aggregation(Seq("y AS y"), Seq("collect(y) AS d"))
      .unwind("c as y")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("e")
      .aggregation(Seq("z AS z"), Seq("collect(z) AS e"))
      .unwind("d as z")
      .aggregation(Seq("y AS y"), Seq("collect(y) AS d"))
      .unwind("c as y")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val nRows = sizeHintToUse
    val input1 = finiteInput(nRows, Some(_ => Array(0)))
    val input2 = finiteInput(nRows, Some(_ => Array(0)))

    // then
    compareMemoryUsageWithInputStreams(logicalQuery1, logicalQuery2, input1, input2)
  }

  test("should deallocate memory between eager") {
    assume(runtime.name.toLowerCase != "pipelined") // Pipelined does not yet support eager

    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .eager()
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .eager()
      .eager()
      .input(variables = Seq("x"))
      .build()

    val nRows = sizeHintToUse

    // then
    compareMemoryUsageWithInputRows(logicalQuery1, logicalQuery2, nRows)
  }

  test("should deallocate memory between sort") {
    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Descending("x")))
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Descending("x")))
      .sort(Seq(Ascending("x")))
      .sort(Seq(Descending("x")))
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x"))
      .build()

    val nRows = sizeHintToUse

    // then
    compareMemoryUsageWithInputRows(logicalQuery1, logicalQuery2, nRows)
  }

  test("should deallocate memory between top") {
    val nRows = sizeHintToUse

    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Descending("x")), nRows/2)
      .top(Seq(Ascending("x")), nRows/2)
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Descending("x")), nRows/2)
      .top(Seq(Ascending("x")), nRows/2)
      .top(Seq(Descending("x")), nRows/2)
      .top(Seq(Ascending("x")), nRows/2)
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputRows(logicalQuery1, logicalQuery2, nRows, toleratedDeviation = 0.1)
  }

  test("should deallocate memory between single node hash joins") {
    // TODO: FIXME Pipelined LHSAccumulatingRHSStreamingSource has a reference counting bug with empty continuations that
    //             causes this test to fail for certain morsel sizes.
    //             We work around this by avoiding such cases to get some test coverage anyway.
    val nNodes = sizeHintToUseWithWorkaroundForPipelined

    given {
      nodeGraph(nNodes)
    }

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .allNodeScan("a")
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .allNodeScan("a")
      .build()

    // then
    compareMemoryUsage(logicalQuery1, logicalQuery2, toleratedDeviation = 0.1)
  }

  test("should deallocate memory between multi node hash joins") {
    // TODO: FIXME Pipelined LHSAccumulatingRHSStreamingSource has a reference counting bug with empty continuations that
    //             causes this test to fail for certain morsel sizes.
    //             We work around this by avoiding such cases to get some test coverage anyway.
    val nNodes = sizeHintToUseWithWorkaroundForPipelined

    val paths = given { chainGraphs(nNodes, "R") }
    val random = new Random(seed = 1337)
    val data = (0 until nNodes).map { i => Array[Any](paths(i).startNode, paths(i).endNode()) }
    val shuffledData = random.shuffle(data).toArray
    val dataFunction = (i: Long) => shuffledData(i.toInt - 1)
    val input1 = finiteInput(nNodes, Some(dataFunction))
    val input2 = finiteInput(nNodes, Some(dataFunction))

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .input(nodes = Seq("a", "b"))
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .input(nodes = Seq("a", "b"))
      .build()

    // then
    compareMemoryUsage(logicalQuery1, logicalQuery2, input1, input2, toleratedDeviation = 0.1)
  }

  test("should deallocate memory between left outer hash joins") {
    assume(runtime.name.toLowerCase != "pipelined") // Pipelined does not yet support outer hash join

    val nNodes = sizeHintToUse

    val nodes = given { nodeGraph(nNodes/2) }
    val random = new Random(seed = 1337)
    val payload: Array[ListValue] = (1 to nNodes).map { _ => VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)}.toArray
    val data = (0 until nNodes/2).map { i => Array[Any](nodes(i), payload(i)) }
    val nullData = (nNodes/2 until nNodes).map { i => Array[Any](VirtualValues.node(-1L), payload(i)) }
    val shuffledData = random.shuffle(data ++ nullData).toArray
    val dataFunction = (i: Long) => shuffledData(i.toInt - 1)
    val input1 = finiteInput(nNodes, Some(dataFunction))
    val input2 = finiteInput(nNodes, Some(dataFunction))

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a", "a2")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .input(nodes = Seq("a"), variables = Seq("payload"))
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a", "a2")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .input(nodes = Seq("a"), variables = Seq("payload"))
      .build()

    // then
    compareMemoryUsage(logicalQuery1, logicalQuery2, input1, input2, toleratedDeviation = 0.1)
  }

  test("should deallocate memory between right outer hash joins") {
    assume(runtime.name.toLowerCase != "pipelined") // Pipelined does not yet support outer hash join

    val nNodes = sizeHintToUse

    val nodes = given { nodeGraph(nNodes/2) }
    val random = new Random(seed = 1337)
    val payload: Array[ListValue] = (1 to nNodes).map { _ => VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)}.toArray
    val data = (0 until nNodes/2).map { i => Array[Any](nodes(i), payload(i)) }
    val nullData = (nNodes/2 until nNodes).map { i => Array[Any](VirtualValues.node(-1L), payload(i)) }
    val shuffledData = random.shuffle(data ++ nullData).toArray
    val dataFunction = (i: Long) => shuffledData(i.toInt - 1)
    val input1 = finiteInput(nNodes, Some(dataFunction))
    val input2 = finiteInput(nNodes, Some(dataFunction))

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a", "a2")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .input(nodes = Seq("a"), variables = Seq("payload"))
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a", "a2")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .input(nodes = Seq("a"), variables = Seq("payload"))
      .build()

    // then
    compareMemoryUsage(logicalQuery1, logicalQuery2, input1, input2, toleratedDeviation = 0.1)
  }

  test("should deallocate memory between value hash joins") {
    // TODO: FIXME Pipelined LHSAccumulatingRHSStreamingSource has a reference counting bug with empty continuations that
    //             causes this test to fail for certain morsel sizes.
    //             We work around this by avoiding such cases to get some test coverage anyway.
    val nNodes = sizeHintToUseWithWorkaroundForPipelined

    given {
      val nodes = nodeGraph(nNodes)
      nodes.foreach(n => n.setProperty("prop",  n.getId))
    }

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    // then
    compareMemoryUsage(logicalQuery1, logicalQuery2, toleratedDeviation = 0.1)
  }

  private def compareMemoryUsage(logicalQuery1: LogicalQuery, logicalQuery2: LogicalQuery, toleratedDeviation: Double = 0.0d): Unit = {
    compareMemoryUsage(logicalQuery1, logicalQuery2, NoInput, NoInput, toleratedDeviation)
  }

  private def compareMemoryUsageWithInputRows(logicalQuery1: LogicalQuery, logicalQuery2: LogicalQuery, nRows: Int, toleratedDeviation: Double = 0.0d): Unit = {
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows)
    compareMemoryUsage(logicalQuery1, logicalQuery2, input1, input2, toleratedDeviation)
  }

  private def compareMemoryUsageWithInputStreams(logicalQuery1: LogicalQuery, logicalQuery2: LogicalQuery, input1: InputDataStream, input2: InputDataStream,
                                                 toleratedDeviation: Double = 0.0d): Unit = {
    compareMemoryUsage(logicalQuery1, logicalQuery2, input1, input2, toleratedDeviation)
  }

  private def compareMemoryUsage(logicalQuery1: LogicalQuery, logicalQuery2: LogicalQuery, input1: InputDataStream, input2: InputDataStream,
                                 toleratedDeviation: Double): Unit = {
    restartTx()
    val runtimeResult1 = profile(logicalQuery1, runtime, input1)
    consume(runtimeResult1)
    val queryProfile1 = runtimeResult1.runtimeResult.queryProfile()
    val maxMem1 = queryProfile1.maxAllocatedMemory()

    //println(s"\nQuery 1 used $maxMem1\n======================================================================================\n")

    restartTx()
    val runtimeResult2 = profile(logicalQuery2, runtime, input2)
    consume(runtimeResult2)
    val queryProfile2 = runtimeResult2.runtimeResult.queryProfile()
    val maxMem2 = queryProfile2.maxAllocatedMemory()
    val memDiff = Math.abs(maxMem2 - maxMem1)

    //println(s"Query 1 used $maxMem1 bytes and Query 2 used $maxMem2 bytes ($memDiff bytes difference)")

    maxMem1 shouldNot be(0)
    val deviation = Math.abs(maxMem1 - maxMem2) / maxMem1.toDouble
    val deviationPercentage = Math.round(deviation * 100)
    val toleratedDeviationPercentage = Math.round(toleratedDeviation * 100)
    val deviationMessage = s"$deviationPercentage%${if (toleratedDeviation > 0.0d) s" is more than tolerated ${toleratedDeviationPercentage}%" else ""}"

    withClue(s"Query 1 used $maxMem1 bytes and Query 2 used $maxMem2 bytes ($memDiff bytes difference, $deviationMessage):\n") {
      if (toleratedDeviation == 0.0d) {
        maxMem1 shouldEqual maxMem2
      } else {
        deviation <= toleratedDeviation shouldBe true
      }
    }
  }
}
