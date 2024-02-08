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
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.InvalidArgumentException

abstract class SkipTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("skip 0") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .skip(0)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity)

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)

    runtimeResult should beColumns("x").withRows(input.flatten)
  }

  test("skip -1") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .skip(-1)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity).stream()

    // then
    val exception = intercept[InvalidArgumentException] {
      consume(execute(logicalQuery, runtime, input))
    }
    exception.getMessage should include("Must be a non-negative integer")
  }

  test("skip Long.MaxValue") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .skip(Long.MaxValue)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity)

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)

    runtimeResult should beColumns("x").withNoRows()
  }

  test("skip -1 on an empty input") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .skip(-1)
      .input(variables = Seq("x"))
      .build()

    val input = inputValues()

    // then
    val exception = intercept[InvalidArgumentException] {
      consume(execute(logicalQuery, runtime, input))
    }
    exception.getMessage should include("Must be a non-negative integer")
  }

  test("skip higher than amount of rows") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .skip(Int.MaxValue)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity)

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should support skip") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .skip(10)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity)

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x").withRows(rowCount(3 * sizeHint - 10))
  }

  test("should support skip with null values") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .skip(10)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, x => if (x % 2 == 0) x else null)

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x").withRows(rowCount(3 * sizeHint - 10))
  }

  test("should support skip in the first of two pipelines") {
    // given
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-->(y)")
      .skip(91)
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withRows(rowCount(900))
  }

  test("should support apply-skip") {
    // given
    val nodesPerLabel = 100
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.skip(90)
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(singleColumn(aNodes.flatMap(n => List().padTo(10, n))))
  }

  test("should support skip on top of apply") {
    // given
    val nodesPerLabel = 50
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val toSkip = nodesPerLabel * nodesPerLabel - 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .skip(toSkip)
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(rowCount(1))
  }

  test("should support reduce -> skip on the RHS of apply") {
    // given
    val nodesPerLabel = 100
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.skip(90)
      .|.sort("y ASC")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      x <- aNodes
      y <- bNodes.sortBy(_.getId).drop(90)
    } yield Array[Any](x, y)

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should support skip -> reduce on the RHS of apply") {
    // given
    val NODES_PER_LABEL = 100
    val SKIP = 90
    givenGraph { bipartiteGraph(NODES_PER_LABEL, "A", "B", "R") }

    // NOTE: Parallel runtime does not guarantee order is preserved across an apply scope

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .planIf(isParallel)(_.sort("y ASC")) // Insert a top-level sort in parallel runtime
      .apply()
      .|.sort("y ASC")
      .|.skip(SKIP)
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val rowOrderMatcher = if (isParallel) sortedAsc("y") else groupedBy(NODES_PER_LABEL, 10, "x").asc("y")
    runtimeResult should beColumns("x", "y").withRows(rowOrderMatcher)
  }

  test("should support chained skips") {
    // given
    val nodesPerLabel = 10
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a2")
      .skip(90)
      .expandAll("(b2)<--(a2)")
      .skip(90)
      .expandAll("(a1)-->(b2)")
      .skip(90)
      .expandAll("(b1)<--(a1)")
      .skip(90)
      .expandAll("(x)-->(b1)")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a2").withRows(rowCount(10))
  }

  test("should support chained skips in the same pipeline") {
    // given
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .skip(188)
      .skip(200)
      .skip(600)
      .expandAll("(b1)<--(a1)")
      .skip(190)
      .skip(300)
      .skip(500)
      .expandAll("(x)-->(b1)")
      .skip(10)
      .skip(30)
      .skip(50)
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a1").withRows(rowCount(12))
  }

  test("should support chained skip and limits") {
    // given
    val nodesPerLabel = 10
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a2")
      .skip(80)
      .limit(90)
      .expandAll("(b2)<--(a2)")
      .limit(10)
      .skip(10)
      .expandAll("(a1)-->(b2)")
      .skip(80)
      .limit(90)
      .expandAll("(b1)<--(a1)")
      .limit(10)
      .skip(10)
      .expandAll("(x)-->(b1)")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a2").withRows(rowCount(10))
  }

  test("SKIP combined with fused-over-pipelines") {
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "rel", "y")
      .cartesianProduct()
      .|.argument()
      .expand("(x)-[rel]->(y)")
      .skip(99)
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x", "rel", "y").withRows(rowCount(nodesPerLabel))
  }

  test("SKIP followed by aggregation") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("sum")
      .aggregation(Seq.empty, Seq("sum(x) AS sum"))
      .skip(10)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, _ => 11)

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("sum").withSingleRow(input.flatten.drop(10).foldLeft(0)((sum, current) =>
      sum + current(0).asInstanceOf[Int]
    ))

  }

  test("should work with aggregation on the RHS of an apply") {
    // given
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        properties = {
          case _: Int => Map("foo" -> s"bar")
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("group", "c")
      .apply()
      .|.aggregation(Seq("n.foo AS group"), Seq("count(n) AS c"))
      .|.skip(sizeHint - 10)
      .|.allNodeScan("n", "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(Array[Any](_)): _*))

    val expected = for { _ <- 1 to 10 } yield Array[Any](s"bar", 10)

    // then
    runtimeResult should beColumns("group", "c").withRows(expected)
  }

  test("should work with chained skips on the RHS of an apply") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        properties = {
          case _: Int => Map("foo" -> s"bar")
        },
        "A"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("bar")
      .projection("a.foo AS bar")
      .apply()
      .|.skip(2)
      .|.skip(sizeHint - 10)
      .|.nodeByLabelScan("a", "A", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(Array[Any](_)): _*))

    // then
    val expected = for {
      _ <- 1 to 10
      _ <- 0 until 8
    } yield s"bar"
    runtimeResult should beColumns("bar").withRows(singleColumn(expected))
  }
}
