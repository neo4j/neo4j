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

import java.util.Collections

import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class OrderedAggregationTestBase[CONTEXT <: RuntimeContext](
                                                                      edition: Edition[CONTEXT],
                                                                      runtime: CypherRuntime[CONTEXT],
                                                                      sizeHint: Int
                                                                    ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should count(*) on single ordered grouping column") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, rowNumber => rowNumber / 10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .orderedAggregation(Seq("x AS x"), Seq("count(*) AS c"), Seq(varFor("x")))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "c").withRows(for (i <- 0 until sizeHint) yield {
      Array(i, 10)
    })
  }

  test("should count(*) on single primitive ordered grouping column") {
    // given
    val (nodes, _) = given { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .orderedAggregation(Seq("x AS x"), Seq("count(*) AS c"), Seq(varFor("x")))
      .sort(Seq(Ascending("x")))
      .expand("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "c").withRows(nodes.map { node =>
      Array(node, 2)
    })
  }

  test("should count(*) on single ordered grouping column with nulls") {
    given {
      nodePropertyGraph(sizeHint, {
        case i: Int if i % 2 == 0 => Map("num" -> i, "name" -> s"bob${i % 10}")
        case i: Int if i % 2 == 1 => Map("num" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "c")
      .orderedAggregation(Seq("name AS name"), Seq("count(*) AS c"), Seq(varFor("name")))
      .sort(Seq(Ascending("name")))
      .projection("x.name AS name")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("name", "c").withRows((for (i <- 0 until 10 by 2) yield {
      Array(s"bob$i", sizeHint / 10)
    }) :+ Array(null, sizeHint / 2))
  }

  test("should count(*) on single primitive ordered grouping column with nulls") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .orderedAggregation(Seq("x AS x"), Seq("count(*) AS c"), Seq(varFor("x")))
      .sort(Seq(Ascending("x")))
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for(node <- nodes if node != null) yield Array(node, 2)
    runtimeResult should beColumns("x", "c").withRows(expected)
  }

  test("should count(*) on two grouping columns, one ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 10, y => y % 2)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("count(*) AS c"), Seq(varFor("x")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "c").withRows(
      for {x <- 0 until sizeHint
           y <- 0 to 1} yield {
        Array(x, y, 5) // (x / 10, y % 2) as grouping gives 5 duplicates per group
      })
  }

  test("should count(*) on two grouping columns, two ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 100, y => (y % 100) / 10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("count(*) AS c"), Seq(varFor("x"), varFor("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "c").withRows(
      for {x <- 0 until sizeHint / 10
           y <- 0 until 10} yield {
        Array(x, y, 10) // (x / 100, (y % 100) / 10) as grouping gives 10 duplicates per group
      })
  }

  test("should count(*) on two primitive grouping columns with nulls, one ordered") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .orderedAggregation(Seq("x AS x", "x2 AS x2"), Seq("count(*) AS c"), Seq(varFor("x")))
      .sort(Seq(Ascending("x")))
      .projection("x AS x2")
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for(node <- nodes if node != null) yield Array(node, 2)
    runtimeResult should beColumns("x", "c").withRows(expected)
  }

  test("should count(*) on two primitive grouping columns with nulls, two ordered") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .orderedAggregation(Seq("x AS x", "x2 AS x2"), Seq("count(*) AS c"), Seq(varFor("x"), varFor("x2")))
      .sort(Seq(Ascending("x")))
      .projection("x AS x2")
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for(node <- nodes if node != null) yield Array(node, 2)
    runtimeResult should beColumns("x", "c").withRows(expected)
  }

  test("should count(*) on three grouping columns, one ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 10, y => y % 2, z => z % 2)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "c")
      .orderedAggregation(Seq("x AS x", "y AS y", "z AS z"), Seq("count(*) AS c"), Seq(varFor("x")))
      .input(variables = Seq("x", "y", "z"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z", "c").withRows(
      for {x <- 0 until sizeHint
           yz <- 0 to 1} yield {
        Array(x, yz, yz, 5) //  (x / 10, y % 2, z % 2) as grouping gives 5 duplicates per group
      })
  }

  test("should count(*) on three grouping columns, two ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, _ / 100, i => (i % 100) / 10, _ % 2)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "c")
      .orderedAggregation(Seq("x AS x", "y AS y", "z AS z"), Seq("count(*) AS c"), Seq(varFor("x"), varFor("y")))
      .input(variables = Seq("x", "y", "z"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z", "c").withRows(
      for {x <- 0 until sizeHint / 10
           y <- 0 until 10
           z <- 0 to 1} yield {
        Array(x, y, z, 5) //  (x / 100, (y % 100) / 10, z % 2) as grouping gives 5 duplicates per group
      })
  }

  test("should count(*) on three grouping columns, three ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 100, y => (y % 100) / 10, z => (z % 10) / 5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "c")
      .orderedAggregation(Seq("x AS x", "y AS y", "z AS z"), Seq("count(*) AS c"), Seq(varFor("x"), varFor("y"), varFor("z")))
      .input(variables = Seq("x", "y", "z"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z", "c").withRows(
      for {x <- 0 until sizeHint / 10
           y <- 0 until 10
           z <- 0 to 1} yield {
        Array(x, y, z, 5) //  (x / 100, (y % 100) / 10, (z % 10) / 5) as grouping gives 5 duplicates per group
      })
  }

  test("should sum(x) on two grouping columns, two ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 100, y => (y % 100) / 10, z => z % 5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("sum(z) AS c"), Seq(varFor("x"), varFor("y")))
      .input(variables = Seq("x", "y", "z"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "c").withRows(
      for {i <- 0 until sizeHint / 10
           j <- 0 until 10} yield {
        Array(i, j, List(0, 1, 2, 3, 4, 0, 1, 2, 3, 4).sum) //  (x / 100, (y % 100) / 10) as grouping gives 10 duplicates per group, with the z values [0,1,2,3,4,0,1,2,3,4]
      })
  }

  test("should return nothing for empty input") {
    // given nothing

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("countStar", "count", "avg", "collect", "max", "min", "sum")
      .orderedAggregation(Seq("x AS x"), Seq(
        "count(*) AS countStar",
        "count(x.num) AS count",
        "avg(x.num) AS avg",
        "collect(x.num) AS collect",
        "max(x.num) AS max",
        "min(x.num) AS min",
        "sum(x.num) AS sum",
      ),
        Seq(varFor("x")))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues())

    // then
    runtimeResult should beColumns("countStar", "count", "avg", "collect", "max", "min", "sum").withNoRows()
  }

  test("should return one row for one input row") {
    // given nothing
    val input = inputValues(Array(1))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("countStar", "count", "avg", "collect", "max", "min", "sum")
      .orderedAggregation(Seq("x AS x"), Seq(
        "count(*) AS countStar",
        "count(x) AS count",
        "avg(x) AS avg",
        "collect(x) AS collect",
        "max(x) AS max",
        "min(x) AS min",
        "sum(x) AS sum",
      ),
        Seq(varFor("x")))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("countStar", "count", "avg", "collect", "max", "min", "sum")
        .withSingleRow(1, 1, 1, Collections.singletonList(1), 1, 1, 1)
  }

  test("should keep input order") {
    // given
    val input = inputColumns(10, sizeHint / 10, identity)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "count")
      .orderedAggregation(Seq("i AS i"), Seq("count(i) AS count"), Seq(varFor("i")))
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("i", "count")
      .withRows(inOrder((0 until sizeHint).map(Array[Any](_, 1))))
  }
}
