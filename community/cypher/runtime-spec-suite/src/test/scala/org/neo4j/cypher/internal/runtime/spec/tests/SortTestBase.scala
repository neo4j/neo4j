/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingProbe
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
      .sort(Seq(Ascending(varFor("x")), Ascending(varFor("y"))))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should handle null values, one column") {
    val unfilteredNodes = given { nodeGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.52)
    val input = inputValues(nodes.map(n => Array[Any](n)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending(varFor("x"))))
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
      .sort(Seq(Descending(varFor("a")), Ascending(varFor("b")), Descending(varFor("c"))))
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
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.sort(Seq(Ascending(varFor("b"))))
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
    val nodes = given { nodeGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(sortItems = Seq(Ascending(varFor("x"))))
      .sort(sortItems = Seq(Descending(varFor("x"))))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(singleColumnInOrder(nodes))
  }

  test("should sort on top of apply with all node scan and sort on rhs of apply") {
    // given
    val nodes = given { nodeGraph(10) }
    val inputRows = inputValues(nodes.map(node => Array[Any](node)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(sortItems = Seq(Descending(varFor("x"))))
      .apply()
      .|.sort(sortItems = Seq(Descending(varFor("x"))))
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputRows)

    runtimeResult should beColumns("x").withRows(rowCount(100))
  }

  test("should sort on top of apply") {
    given { circleGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort(Seq(Descending(varFor("y"))))
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
    given { circleGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.apply()
      .|.|.sort(Seq(Ascending(varFor("z"))))
      .|.|.expandAll("(y)--(z)")
      .|.|.argument()
      .|.sort(Seq(Descending(varFor("y"))))
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(groupedBy("x", "y").asc("z"))
  }

  test("should handle sort after distinct removed rows (FilteringExecutionContext and cancelled rows) I") {
    // this test is mostly testing pipelined runtime and assumes a morsel/batch size of 4
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("A")
      .sort(Seq(Ascending(varFor("A"))))
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
      .sort(Seq(Ascending(varFor("B"))))
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
      .sort(Seq(Ascending(varFor("keep"))))
      .prober(probe1)
      .projection(project = Seq("keep as keep"), discard = Set("discard"))
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
      sortedRange.map(i => Seq(stringValue(s"bla$i"), null))
  }

  // Sort do not break in slotted, so should not discard
  test("should not discard columns (slotted)") {
    assume(runtime.name == "slotted")

    val probe1 = RecordingProbe("keep", "discard")
    val probe2 = RecordingProbe("keep", "discard")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("keep")
      .prober(probe2)
      // We discard here but should not remove because there's no eager buffer after this point
      .projection(project = Seq("0 as hi"), discard = Set("keep"))
      .sort(Seq(Ascending(varFor("keep"))))
      .prober(probe1)
      .projection(project = Seq("keep as keep"), discard = Set("discard"))
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
