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
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.storable.Values.stringValue

abstract class SortTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("empty input gives empty output") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort("x ASC", "y ASC")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should handle null values, one column") {
    val unfilteredNodes = givenGraph { nodeGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.52)
    val input = inputValues(nodes.map(n => Array[Any](n)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x ASC")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = nodes.sortBy(n => if (n == null) Long.MaxValue else n.getId)
    runtimeResult should beColumns("x").withRows(inOrder(expected.map(x => Array[Any](x))))
  }

  test("should sort many columns") {
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 2, i % 4, i)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .sort("a DESC", "b ASC", "c DESC")
      .input(variables = Seq("a", "b", "c"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      input.flatten.sortBy(arr => (-arr(0).asInstanceOf[Int], arr(1).asInstanceOf[Int], -arr(2).asInstanceOf[Int]))
    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }

  test("should sort under apply") {
    val nodesPerLabel = 100
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.sort("b ASC")
      .|.expandAll("(a)-->(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- aNodes
      y <- bNodes.sortBy(_.getId)
    } yield Array[Any](x, y)

    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should sort twice in a row") {
    // given
    val nodes = givenGraph { nodeGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x ASC")
      .sort("x DESC")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(singleColumnInOrder(nodes))
  }

  test("should sort on top of apply with all node scan and sort on rhs of apply") {
    // given
    val nodes = givenGraph { nodeGraph(10) }
    val inputRows = inputValues(nodes.map(node => Array[Any](node)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x DESC")
      .apply()
      .|.sort("x DESC")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputRows)

    runtimeResult should beColumns("x").withRows(rowCount(100))
  }

  test("should sort on top of apply") {
    givenGraph { circleGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort("y DESC")
      .apply()
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(sortedDesc("y"))
  }

  test("should apply apply sort") {
    givenGraph { circleGraph(1000) }

    // NOTE: Parallel runtime does not guarantee order is preserved across an apply scope.

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .planIf(isParallel)(_.sort("z ASC")) // Insert a top-level sort in parallel runtime
      .apply()
      .|.apply()
      .|.|.sort("z ASC")
      .|.|.expandAll("(y)--(z)")
      .|.|.argument()
      .|.sort("y DESC")
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val rowOrderMatcher = if (isParallel) sortedAsc("z") else groupedBy("x", "y").asc("z")
    runtimeResult should beColumns("x", "y", "z").withRows(rowOrderMatcher)
  }

  test("should handle sort after distinct removed rows (FilteringExecutionContext and cancelled rows) I") {
    // this test is mostly testing pipelined runtime and assumes a morsel/batch size of 4
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("A")
      .sort("A ASC")
      .distinct("x AS A")
      .input(variables = Seq("x"))
      .build()

    // when (one element in each batch will be cancelled)
    val runtimeResult =
      execute(logicalQuery, runtime, inputValues(Array(4), Array(2), Array(4), Array(1), Array(3), Array(5), Array(3)))

    // then
    runtimeResult should beColumns("A").withRows(List(Array(1), Array(2), Array(3), Array(4), Array(5)))
  }

  test("should handle sort after distinct removed rows (FilteringExecutionContext and cancelled rows) II") {
    // this test is mostly testing pipelined runtime and assumes a morsel/batch size of 4
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("A")
      .sort("B ASC")
      .projection("A AS A", "A AS B")
      .distinct("x AS A")
      .input(variables = Seq("x"))
      .build()

    // when (the first element of the second batch will be cancelled)
    val runtimeResult =
      execute(logicalQuery, runtime, inputValues(Array(1), Array(2), Array(3), Array(4), Array(4), Array(5), Array(6)))

    // then
    runtimeResult should beColumns("A").withRows(List(Array(1), Array(2), Array(3), Array(4), Array(5), Array(6)))
  }

  test("should discard columns") {
    assume(runtime.name != "interpreted" && runtime.name != "slotted")

    val probe1 = recordingProbe("keep", "discard")
    val probe2 = recordingProbe("keep", "discard")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("keep")
      .prober(probe2)
      .nonFuseable() // Needed because of limitation in prober
      .sort("keep ASC")
      .prober(probe1)
      .projection("keep as keep")
      .projection("'bla' + a as keep", "'blö' + a as discard")
      .unwind(s"range(0, $sizeHint) AS a")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    val sortedRange = Range.inclusive(0, sizeHint)
      .map(_.toString)
      .sorted
    result should beColumns("keep")
      .withRows(inOrder(sortedRange.map(i => Array(s"bla$i"))))

    probe1.seenRows.map(_.toSeq).toSet shouldBe
      Range.inclusive(0, sizeHint)
        .map(i => Seq(stringValue(s"bla$i"), stringValue(s"blö$i"))).toSet

    probe2.seenRows.map(_.toSeq).toSeq shouldBe
      sortedRange.map(i => Seq(stringValue(s"bla$i"), null))
  }

  test("should discard unused columns") {
    val probe1 = recordingProbe("keep", "discard")
    val probe2 = recordingProbe("keep", "discard")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("keep")
      .prober(probe2)
      .nonFuseable() // Needed because of limitation in prober
      .sort("keep ASC")
      .prober(probe1)
      .projection("'bla' + a as keep", "'blö' + a as discard")
      .unwind(s"range(0, $sizeHint) AS a")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    val sortedRange = Range.inclusive(0, sizeHint)
      .map(_.toString)
      .sorted
    result should beColumns("keep")
      .withRows(inOrder(sortedRange.map(i => Array(s"bla$i"))))

    if (runtimeUsed == Interpreted || runtimeUsed == Slotted) {
      // Sort do not break in slotted
      probe1.seenRows.map(_.toSeq).toSeq shouldBe
        Range.inclusive(0, sizeHint)
          .map(i => Seq(stringValue(s"bla$i"), stringValue(s"blö$i")))

      probe2.seenRows.map(_.toSeq).toSeq shouldBe
        sortedRange.map(i => Seq(stringValue(s"bla$i"), stringValue(s"blö$i")))
    } else if (runtimeUsed != Parallel) {
      probe1.seenRows.map(_.toSeq).toSeq shouldBe
        Range.inclusive(0, sizeHint)
          .map(i => Seq(stringValue(s"bla$i"), stringValue(s"blö$i")))

      probe2.seenRows.map(_.toSeq).toSeq shouldBe
        sortedRange.map(i => Seq(stringValue(s"bla$i"), null))

    }
  }

  // Sort do not break in slotted, so should not discard
  test("should not discard columns (slotted)") {
    assume(runtime.name == "slotted")

    val probe1 = recordingProbe("keep", "discard")
    val probe2 = recordingProbe("keep", "discard")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("keep")
      .prober(probe2)
      // We discard here but should not remove because there's no eager buffer after this point
      .projection("0 as hi")
      .sort("keep ASC")
      .prober(probe1)
      .projection("keep as keep")
      .projection("'bla' + a as keep", "'blö' + a as discard")
      .unwind(s"range(0, $sizeHint) AS a")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    val sortedRange = Range.inclusive(0, sizeHint)
      .map(_.toString)
      .sorted
    result should beColumns("keep")
      .withRows(inOrder(sortedRange.map(i => Array(s"bla$i"))))

    probe1.seenRows.map(_.toSeq).toSeq shouldBe
      Range.inclusive(0, sizeHint)
        .map(i => Seq(stringValue(s"bla$i"), stringValue(s"blö$i")))

    probe2.seenRows.map(_.toSeq).toSeq shouldBe
      sortedRange.map(i => Seq(stringValue(s"bla$i"), stringValue(s"blö$i")))
  }
}
