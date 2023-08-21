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
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import java.util.Collections

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
      .orderedAggregation(Seq("x AS x"), Seq("count(*) AS c"), Seq("x"))
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
      .orderedAggregation(Seq("x AS x"), Seq("count(*) AS c"), Seq("x"))
      .sort("x ASC")
      .expand("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "c").withRows(nodes.map { node =>
      Array[Any](node, 2)
    })
  }

  test("should count(*) on single ordered grouping column with nulls") {
    given {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i, "name" -> s"bob${i % 10}")
          case i: Int if i % 2 == 1 => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "c")
      .orderedAggregation(Seq("name AS name"), Seq("count(*) AS c"), Seq("name"))
      .sort("name ASC")
      .projection("x.name AS name")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("name", "c").withRows((for (i <- 0 until 10 by 2) yield {
      Array[Any](s"bob$i", sizeHint / 10)
    }) :+ Array[Any](null, sizeHint / 2))
  }

  test("should count(*) on single primitive ordered grouping column with nulls") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .orderedAggregation(Seq("x AS x"), Seq("count(*) AS c"), Seq("x"))
      .sort("x ASC")
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for (node <- nodes if node != null) yield Array[Any](node, 2)
    runtimeResult should beColumns("x", "c").withRows(expected)
  }

  test("should count(*) on two grouping columns, one ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 10, y => y % 2)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("count(*) AS c"), Seq("x"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "c").withRows(
      for {
        x <- 0 until sizeHint
        y <- 0 to 1
      } yield {
        Array(x, y, 5) // (x / 10, y % 2) as grouping gives 5 duplicates per group
      }
    )
  }

  test("should count(*) on two grouping columns, two ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 100, y => (y % 100) / 10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("count(*) AS c"), Seq("x", "y"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "c").withRows(
      for {
        x <- 0 until sizeHint / 10
        y <- 0 until 10
      } yield {
        Array(x, y, 10) // (x / 100, (y % 100) / 10) as grouping gives 10 duplicates per group
      }
    )
  }

  test("should count(*) on two primitive grouping columns with nulls, one ordered") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .orderedAggregation(Seq("x AS x", "x2 AS x2"), Seq("count(*) AS c"), Seq("x"))
      .sort("x ASC")
      .projection("x AS x2")
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for (node <- nodes if node != null) yield Array[Any](node, 2)
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
      .orderedAggregation(Seq("x AS x", "x2 AS x2"), Seq("count(*) AS c"), Seq("x", "x2"))
      .sort("x ASC")
      .projection("x AS x2")
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for (node <- nodes if node != null) yield Array[Any](node, 2)
    runtimeResult should beColumns("x", "c").withRows(expected)
  }

  test("should count(*) on three grouping columns, one ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 10, y => y % 2, z => z % 2)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "c")
      .orderedAggregation(Seq("x AS x", "y AS y", "z AS z"), Seq("count(*) AS c"), Seq("x"))
      .input(variables = Seq("x", "y", "z"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z", "c").withRows(
      for {
        x <- 0 until sizeHint
        yz <- 0 to 1
      } yield {
        Array(x, yz, yz, 5) //  (x / 10, y % 2, z % 2) as grouping gives 5 duplicates per group
      }
    )
  }

  test("should count(*) on three grouping columns, two ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, _ / 100, i => (i % 100) / 10, _ % 2)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "c")
      .orderedAggregation(Seq("x AS x", "y AS y", "z AS z"), Seq("count(*) AS c"), Seq("x", "y"))
      .input(variables = Seq("x", "y", "z"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z", "c").withRows(
      for {
        x <- 0 until sizeHint / 10
        y <- 0 until 10
        z <- 0 to 1
      } yield {
        Array(x, y, z, 5) //  (x / 100, (y % 100) / 10, z % 2) as grouping gives 5 duplicates per group
      }
    )
  }

  test("should count(*) on three grouping columns, three ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 100, y => (y % 100) / 10, z => (z % 10) / 5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "c")
      .orderedAggregation(Seq("x AS x", "y AS y", "z AS z"), Seq("count(*) AS c"), Seq("x", "y", "z"))
      .input(variables = Seq("x", "y", "z"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z", "c").withRows(
      for {
        x <- 0 until sizeHint / 10
        y <- 0 until 10
        z <- 0 to 1
      } yield {
        Array(x, y, z, 5) //  (x / 100, (y % 100) / 10, (z % 10) / 5) as grouping gives 5 duplicates per group
      }
    )
  }

  test("should sum(x) on two grouping columns, two ordered") {
    // given
    val input = inputColumns(nBatches = sizeHint, batchSize = 10, x => x / 100, y => (y % 100) / 10, z => z % 5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("sum(z) AS c"), Seq("x", "y"))
      .input(variables = Seq("x", "y", "z"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "c").withRows(
      for {
        i <- 0 until sizeHint / 10
        j <- 0 until 10
      } yield {
        Array(
          i,
          j,
          List(0, 1, 2, 3, 4, 0, 1, 2, 3, 4).sum
        ) //  (x / 100, (y % 100) / 10) as grouping gives 10 duplicates per group, with the z values [0,1,2,3,4,0,1,2,3,4]
      }
    )
  }

  test("should return nothing for empty input") {
    // given nothing

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(
        "countStar",
        "count",
        "countD",
        "avg",
        "avgD",
        "collect",
        "collectD",
        "max",
        "maxD",
        "min",
        "minD",
        "sum",
        "sumD"
      )
      .orderedAggregation(
        Seq("x AS x"),
        Seq(
          "count(*) AS countStar",
          "count(x.num) AS count",
          "count(DISTINCT x.num) AS countD",
          "avg(x.num) AS avg",
          "avg(DISTINCT x.num) AS avgD",
          "collect(x.num) AS collect",
          "collect(DISTINCT x.num) AS collectD",
          "max(x.num) AS max",
          "max(DISTINCT x.num) AS maxD",
          "min(x.num) AS min",
          "min(DISTINCT x.num) AS minD",
          "sum(x.num) AS sum",
          "sum(DISTINCT x.num) AS sumD"
        ),
        Seq("x")
      )
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues())

    // then
    runtimeResult should beColumns(
      "countStar",
      "count",
      "countD",
      "avg",
      "avgD",
      "collect",
      "collectD",
      "max",
      "maxD",
      "min",
      "minD",
      "sum",
      "sumD"
    ).withNoRows()
  }

  test("should return one row for one input row") {
    // given one row
    val input = inputValues(Array(1))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(
        "countStar",
        "count",
        "countD",
        "avg",
        "avgD",
        "collect",
        "collectD",
        "max",
        "maxD",
        "min",
        "minD",
        "sum",
        "sumD"
      )
      .orderedAggregation(
        Seq("x AS x"),
        Seq(
          "count(*) AS countStar",
          "count(x) AS count",
          "count(DISTINCT x) AS countD",
          "avg(x) AS avg",
          "avg(DISTINCT x) AS avgD",
          "collect(x) AS collect",
          "collect(DISTINCT x) AS collectD",
          "max(x) AS max",
          "max(DISTINCT x) AS maxD",
          "min(x) AS min",
          "min(DISTINCT x) AS minD",
          "sum(x) AS sum",
          "sum(DISTINCT x) AS sumD"
        ),
        Seq("x")
      )
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns(
      "countStar",
      "count",
      "countD",
      "avg",
      "avgD",
      "collect",
      "collectD",
      "max",
      "maxD",
      "min",
      "minD",
      "sum",
      "sumD"
    )
      .withSingleRow(1, 1, 1, 1, 1, Collections.singletonList(1), Collections.singletonList(1), 1, 1, 1, 1, 1, 1)
  }

  test("should keep input order") {
    // given
    val input = inputColumns(10, sizeHint / 10, identity)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "count")
      .orderedAggregation(Seq("i AS i"), Seq("count(i) AS count"), Seq("i"))
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("i", "count")
      .withRows(inOrder((0 until sizeHint).map(Array[Any](_, 1))))
  }

  test("should count on single ordered grouping column under apply") {
    val nodesPerLabel = 100
    nodeIndex("B", "prop")
    val (aNodes, bNodes) = given {
      val aNodes = nodeGraph(nodesPerLabel, "A")
      val bNodes = nodePropertyGraph(
        nodesPerLabel,
        {
          case i: Int => Map("prop" -> (if (i % 2 == 0) 5 else 0))
        },
        "B"
      )
      (aNodes, bNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.orderedAggregation(Seq("b AS b"), Seq("count(number) AS c"), Seq("b"))
      .|.unwind("range(1, b.prop) AS number")
      .|.nodeIndexOperator("b:B(prop >= 0)", indexOrder = IndexOrderAscending, argumentIds = Set("a"))
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- aNodes
      b <- bNodes if b.getProperty("prop").asInstanceOf[Int] == 5
    } yield Array[Any](a, b, 5)

    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }

  test("should handle long chunks") {
    // given
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, rowNumber => rowNumber / 100, identity)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("count(*) AS c"), Seq("x"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = input.flatten.map(row => row :+ 1)
    runtimeResult should beColumns("x", "y", "c").withRows(expected)
  }

  test("should count on single ordered, single unordered grouping column under apply") {
    val propValue = 10
    val nodesPerLabel = 100
    nodeIndex("B", "prop")
    val (aNodes, bNodes) = given {
      val aNodes = nodeGraph(nodesPerLabel, "A")
      val bNodes = nodePropertyGraph(
        nodesPerLabel,
        {
          case i: Int => Map("prop" -> (if (i % 2 == 0) propValue else 0))
        },
        "B"
      )
      (aNodes, bNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "n", "c")
      .apply()
      .|.orderedAggregation(Seq("b AS b", "number AS n"), Seq("count(number) AS c"), Seq("b"))
      .|.unwind("range(1, b.prop) AS number")
      .|.nodeIndexOperator("b:B(prop >= 0)", indexOrder = IndexOrderAscending, argumentIds = Set("a"))
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- aNodes
      b <- bNodes if b.getProperty("prop").asInstanceOf[Int] == propValue
      n <- Range.inclusive(1, propValue)
    } yield Array[Any](a, b, n, 1)

    runtimeResult should beColumns("a", "b", "n", "c").withRows(inOrder(expected))
  }

  test("ordered aggregation should not exhaust input when there is no demand, one column, one ordered") {
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row / 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .orderedAggregation(Seq("x AS x"), Seq("count(*) AS c"), Seq("x"))
      .input(variables = Seq("x"))
      .build()

    val stream = input.stream()
    // When
    val result = execute(logicalQuery, runtime, stream, TestSubscriber.concurrent)

    // Then
    result.request(1)
    result.await() shouldBe true
    // we shouldn't have exhausted the entire input
    stream.hasMore shouldBe true
  }

  test("ordered aggregation should not exhaust input when there is no demand, two columns, one ordered") {
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row / 10, identity)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("count(*) AS c"), Seq("x"))
      .input(variables = Seq("x", "y"))
      .build()

    val stream = input.stream()
    // When
    val result = execute(logicalQuery, runtime, stream, TestSubscriber.concurrent)

    // Then
    result.request(1)
    result.await() shouldBe true
    // we shouldn't have exhausted the entire input
    stream.hasMore shouldBe true
  }

  test("ordered aggregation on rhs should handle lhs rows with extra slots") {

    val query = new LogicalQueryBuilder(this)
      .produceResults("c", "item", "1")
      .projection("1 AS `1`")
      .letSemiApply("item") // RefSlots c, item, 1
      .|.filter("c > 0")
      .|.orderedAggregation(Seq("c as c"), Seq("max(`c`) AS `max(c)`"), Seq("c")) // RefSlots c, max(c)
      .|.argument("c") // RefSlots c
      .unwind("[1,2,3] as c") // RefSlots c, item, 1
      .argument()
      .build()

    val runtimeResult = execute(query, runtime)

    runtimeResult should beColumns("c", "item", "1").withRows(Seq(
      Array(1, true, 1),
      Array(2, true, 1),
      Array(3, true, 1)
    ))
  }

  test("ordered grouping aggregation on rhs should handle lhs rows with extra slots ") {

    val query = new LogicalQueryBuilder(this)
      .produceResults("c", "item", "1")
      .projection("1 AS `1`")
      .letSemiApply("item") // RefSlots c, item, 1
      .|.filter("c > 0")
      .|.orderedAggregation(Seq("c as c"), Seq("max(`c`) AS `max(c)`"), Seq()) // RefSlots c, max(c)
      .|.argument("c") // RefSlots c
      .unwind("[1,2,3] as c") // RefSlots c, item, 1
      .argument()
      .build()

    val runtimeResult = execute(query, runtime)

    runtimeResult should beColumns("c", "item", "1").withRows(Seq(
      Array(1, true, 1),
      Array(2, true, 1),
      Array(3, true, 1)
    ))
  }
}
