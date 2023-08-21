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

abstract class AntiConditionalApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("anti conditional apply should not run rhs if lhs is empty") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x")
      .|.filter("1/0 > 0")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // should not throw 1/0 exception
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("anti conditional apply on nonempty lhs and empty rhs, where condition(lhs) always is false") {
    // given
    given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x")
      .|.expandAll("(y)--(z)")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult =
      execute(logicalQuery, runtime, inputValues(Array[Any](null), Array[Any](null), Array[Any](null)))

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("anti conditional apply on nonempty lhs and empty rhs") {
    // given
    given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x")
      .|.filter("false")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // will only return lhs where condition(lhs) is true
    runtimeResult should beColumns("x", "y").withRows(Seq(Array("42", null), Array("43", null)))
  }

  test("anti conditional apply on nonempty lhs and nonempty rhs") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = (Array[Any]("42", null) +: nodes.map(Array[Any](null, _))) :+ Array[Any]("43", null)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("anti conditional apply on non-nullable node") {
    val nodeCount = sizeHint
    val (nodes, _) = given {
      circleGraph(nodeCount, "L")
    }

    // when
    val limit = 2
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x")
      .|.limit(limit)
      .|.expand("(y)--(z)")
      .|.nodeByLabelScan("y", "L", IndexOrderNone, "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(nodes.map(n => Array[Any](n, null)))
  }

  test("anti conditional apply on the RHS of an apply") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.antiConditionalApply("x")
      .|.|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .|.filter("x = '42' OR x IS NULL")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = Array[Any]("42", null) +: nodes.map(Array[Any](null, _))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should support limit on top of anti conditional apply") {
    // given
    val nodesPerLabel = 50
    val (nodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val input = inputColumns(100000, 3, i => if (i % 2 == 0) nodes(i % nodes.size) else null).stream()

    val limit = nodesPerLabel * nodesPerLabel - 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .limit(limit)
      .antiConditionalApply("x")
      .|.optionalExpandAll("(x)-->(y)")
      .|.argument()
      .input(nodes = Seq("x"), nullable = true)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)

    runtimeResult should beColumns("x", "y").withRows(rowCount(limit))
    if (!isParallel) {
      // parallel runtime may exhaust input before cancellation kicks in (depends on fusing, morsel size, parallelism)
      input.hasMore shouldBe true
    }
  }

  test("should support reduce -> limit on the RHS of anti conditional apply") {
    // given
    val nodesPerLabel = 100
    val (_, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val limit = 10
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "z")
      .antiConditionalApply("x")
      .|.limit(limit)
      .|.sort("z ASC")
      .|.expandAll("(y)-->(z)")
      .|.nodeByLabelScan("y", "A", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null), Array(42)))

    runtimeResult should beColumns("x", "z").withRows(Seq.fill(10)(Array(null, bNodes.head)) :+ Array[Any](42, null))
  }

  test("should aggregate on top of anti conditional apply with expand and limit and aggregation on rhs of apply") {
    // given
    val nodesPerLabel = 10
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("counts")
      .aggregation(Seq.empty, Seq("count(value) AS counts"))
      .antiConditionalApply("x")
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expand("(y)-[:R]->(z)")
      .|.nodeByLabelScan("y", "A", IndexOrderNone, "x")
      .projection("42 AS value")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null), Array(42)))
    runtimeResult should beColumns("counts").withSingleRow(2)
  }

  test("should aggregate with no grouping on top of anti conditional apply with expand on RHS") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("vs")
      .aggregation(Seq.empty, Seq("count(value) AS vs"))
      .antiConditionalApply("x")
      .|.expandAll("(y)-->(z)")
      .|.nodeByLabelScan("y", "A", IndexOrderNone, "x")
      .projection("42 AS value")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null), Array(42)))

    // then
    runtimeResult should beColumns("vs").withSingleRow(aNodes.size * bNodes.size + 1)
  }

  test("should aggregate on top of anti conditional apply with expand on RHS") {
    // given
    val nodesPerLabel = 10
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "ys")
      .aggregation(Seq("x AS x"), Seq("count(y) AS ys"))
      .antiConditionalApply("x")
      .|.expandAll("(y)-->(z)")
      .|.nodeByLabelScan("y", "A", IndexOrderNone, "x")
      .projection("42 AS value")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null), Array(42)))

    // then
    runtimeResult should beColumns("x", "ys").withRows(Seq(
      Array[Any](42, 0),
      Array[Any](null, nodesPerLabel * nodesPerLabel)
    ))
  }

  test("should aggregate on top of anti conditional apply with expand on RHS with nulls") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = given {
      bipartiteGraph(
        nodesPerLabel,
        "A",
        "B",
        "R",
        { case i if i % 2 == 0 => Map("prop" -> i) }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "ys")
      .aggregation(Seq("x AS x"), Seq("count(y) AS ys"))
      .antiConditionalApply("prop")
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .projection("x AS x", "x.prop AS prop")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = aNodes.map {
      case x if !x.hasProperty("prop") => Array[Any](x, bNodes.size)
      case x                           => Array[Any](x, 0)
    }
    runtimeResult should beColumns("x", "ys").withRows(expected)
  }

  test("limit after antiConditionalApply on the RHS of apply") {
    // given
    val inputRows = (0 until 5).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.exhaustiveLimit(1)
      .|.apply()
      .|.|.antiConditionalApply("i")
      .|.|.|.argument("x", "i")
      .|.|.argument("x", "i")
      .|.unwind("[1, null] AS i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(singleColumn(inputRows.map(_(0))))
  }
}

trait OrderedAntiConditionalApplyTestBase[CONTEXT <: RuntimeContext] {
  self: AntiConditionalApplyTestBase[CONTEXT] =>

  test("anti conditional apply should not run rhs if lhs is empty - with leveraged order") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x").withLeveragedOrder()
      .|.filter("1/0 > 0")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // should not throw 1/0 exception
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test(
    "anti conditional apply on nonempty lhs and empty rhs, where condition(lhs) always is false - with leveraged order"
  ) {
    // given
    given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x").withLeveragedOrder()
      .|.expandAll("(y)--(z)")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult =
      execute(logicalQuery, runtime, inputValues(Array[Any](null), Array[Any](null), Array[Any](null)))

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("anti conditional apply on nonempty lhs and empty rhs - with leveraged order") {
    // given
    given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x").withLeveragedOrder()
      .|.filter("false")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // will only return lhs where condition(lhs) is true
    runtimeResult should beColumns("x", "y").withRows(inOrder(Seq(Array("42", null), Array("43", null))))
  }

  test("anti conditional apply on nonempty lhs and nonempty rhs  - with leveraged order") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x").withLeveragedOrder()
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = (Array[Any]("42", null) +: nodes.map(Array[Any](null, _))) :+ Array[Any]("43", null)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("anti conditional apply on non-nullable node - with leveraged order") {
    val nodeCount = sizeHint
    val (nodes, _) = given {
      circleGraph(nodeCount, "L")
    }

    // when
    val limit = 2
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("x").withLeveragedOrder()
      .|.limit(limit)
      .|.expand("(y)--(z)")
      .|.nodeByLabelScan("y", "L", IndexOrderNone, "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(nodes.map(n => Array[Any](n, null)))
  }

  test("anti conditional apply on the RHS of an apply - with leveraged order") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.antiConditionalApply("x").withLeveragedOrder()
      .|.|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .|.filter("x = '42' OR x IS NULL")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = Array[Any]("42", null) +: nodes.map(Array[Any](null, _))
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("should support limit on top of anti conditional apply - with leveraged order") {
    // given
    val nodesPerLabel = 50
    val (nodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val input = inputColumns(100000, 3, i => if (i % 2 == 0) nodes(i % nodes.size) else null).stream()

    val limit = nodesPerLabel * nodesPerLabel - 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .limit(limit)
      .antiConditionalApply("x").withLeveragedOrder()
      .|.optionalExpandAll("(x)-->(y)")
      .|.argument()
      .input(nodes = Seq("x"), nullable = true)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)

    runtimeResult should beColumns("x", "y").withRows(rowCount(limit))
    if (!isParallel) {
      // parallel runtime may exhaust input before cancellation kicks in (depends on fusing, morsel size, parallelism)
      input.hasMore shouldBe true
    }
  }

  test("should support reduce -> limit on the RHS of anti conditional apply - with leveraged order") {
    // given
    val nodesPerLabel = 100
    val (_, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val limit = 10
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "z")
      .antiConditionalApply("x").withLeveragedOrder()
      .|.limit(limit)
      .|.sort("z ASC")
      .|.expandAll("(y)-->(z)")
      .|.nodeByLabelScan("y", "A", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null), Array(42)))

    runtimeResult should beColumns("x", "z").withRows(inOrder(Seq.fill(10)(Array(null, bNodes.head)) :+ Array[Any](
      42,
      null
    )))
  }

  test(
    "should aggregate on top of anti conditional apply with expand and limit and aggregation on rhs of apply - with leveraged order"
  ) {
    // given
    val nodesPerLabel = 10
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("counts")
      .aggregation(Seq.empty, Seq("count(value) AS counts"))
      .antiConditionalApply("x").withLeveragedOrder()
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expand("(y)-[:R]->(z)")
      .|.nodeByLabelScan("y", "A", IndexOrderNone, "x")
      .projection("42 AS value")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null), Array(42)))
    runtimeResult should beColumns("counts").withSingleRow(2)
  }

  test("should aggregate with no grouping on top of anti conditional apply with expand on RHS - with leveraged order") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("vs")
      .aggregation(Seq.empty, Seq("count(value) AS vs"))
      .antiConditionalApply("x").withLeveragedOrder()
      .|.expandAll("(y)-->(z)")
      .|.nodeByLabelScan("y", "A", IndexOrderNone, "x")
      .projection("42 AS value")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null), Array(42)))

    // then
    runtimeResult should beColumns("vs").withSingleRow(aNodes.size * bNodes.size + 1)
  }

  test("should aggregate on top of anti conditional apply with expand on RHS - with leveraged order") {
    // given
    val nodesPerLabel = 10
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "ys")
      .aggregation(Seq("x AS x"), Seq("count(y) AS ys"))
      .antiConditionalApply("x").withLeveragedOrder()
      .|.expandAll("(y)-->(z)")
      .|.nodeByLabelScan("y", "A", IndexOrderNone, "x")
      .projection("42 AS value")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null), Array(42)))

    // then
    runtimeResult should beColumns("x", "ys").withRows(Seq(
      Array[Any](42, 0),
      Array[Any](null, nodesPerLabel * nodesPerLabel)
    ))
  }

  test("should aggregate on top of anti conditional apply with expand on RHS with nulls - with leveraged order") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = given {
      bipartiteGraph(
        nodesPerLabel,
        "A",
        "B",
        "R",
        { case i if i % 2 == 0 => Map("prop" -> i) }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "ys")
      .aggregation(Seq("x AS x"), Seq("count(y) AS ys"))
      .antiConditionalApply("prop").withLeveragedOrder()
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .projection("x AS x", "x.prop AS prop")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = aNodes.map {
      case x if !x.hasProperty("prop") => Array[Any](x, bNodes.size)
      case x                           => Array[Any](x, 0)
    }
    runtimeResult should beColumns("x", "ys").withRows(expected)
  }

  test("limit after antiConditionalApply on the RHS of apply - with leveraged order") {
    // given
    val inputRows = (0 until 5).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.exhaustiveLimit(1)
      .|.apply()
      .|.|.antiConditionalApply("i").withLeveragedOrder()
      .|.|.|.argument("x", "i")
      .|.|.argument("x", "i")
      .|.unwind("[1, null] AS i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(singleColumn(inputRows.map(_(0))))
  }
}
