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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.builder.Parser
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class ProvidedOrderTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime)
    with RandomValuesTestSupport {

  trait SeqMutator { def apply[X](in: Seq[X]): Seq[X] }

  case class ProvidedOrderTest(
    orderString: String,
    indexOrder: IndexOrder,
    providedOrderFactory: String => ProvidedOrder,
    expectedMutation: SeqMutator
  )
}

trait NonParallelProvidedOrderTestBase[CONTEXT <: RuntimeContext] {
  self: ProvidedOrderTestBase[CONTEXT] =>

  private[this] val parse: String => Expression = Parser.parseExpression
  private[this] val asc: Expression => ProvidedOrder = ProvidedOrder.asc(_: Expression)
  private[this] val desc: Expression => ProvidedOrder = ProvidedOrder.desc(_: Expression)

  for (
    ProvidedOrderTest(orderString, indexOrder, providedOrderFactory, expectedMutation) <- Seq(
      ProvidedOrderTest(
        "ascending",
        IndexOrderAscending,
        parse andThen asc,
        new SeqMutator {
          override def apply[X](in: Seq[X]): Seq[X] = in
        }
      ),
      ProvidedOrderTest(
        "descending",
        IndexOrderDescending,
        parse andThen desc,
        new SeqMutator {
          override def apply[X](in: Seq[X]): Seq[X] = in.reverse
        }
      )
    )
  ) {

    test(s"expand keeps index provided $orderString order") {
      // given
      val n = sizeHint
      val relTuples = (for (i <- 0 until n) yield {
        Seq(
          (i, (2 * i) % n, "OTHER"),
          (i, (3 * i) % n, "OTHER"),
          (i, (4 * i) % n, "OTHER"),
          (i, (5 * i) % n, "OTHER"),
          (i, (i + 1) % n, "NEXT"),
          (i, i, "SELF")
        )
      }).reduce(_ ++ _)
      val nodes = givenGraph {
        nodeIndex("Honey", "prop")
        val nodes = nodePropertyGraph(
          n,
          {
            case i if i % 10 == 0 => Map("prop" -> i)
          },
          "Honey"
        )
        connect(nodes, relTuples)
        nodes
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop").withLeveragedOrder()
        .projection("x.prop AS prop")
        .expand("(y)-->(z)") // 6x more rows
        .expand("(x)-->(y)") // 6x more rows
        .nodeIndexOperator(
          s"x:Honey(prop > ${sizeHint / 2})",
          indexOrder = indexOrder,
          getValue = _ => DoNotGetValue
        ).withProvidedOrder(providedOrderFactory("x.prop"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = expectedMutation(nodes.zipWithIndex.filter { case (_, i) => i % 10 == 0 && i > n / 2 }.flatMap(n =>
        Seq.fill(6 * 6)(n)
      ).map(_._2))
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"cartesian product keeps LHS and RHS provided $orderString order with apply") {
      // given
      val n = sizeHint / 10
      val modulo = 100
      val zGreaterThanFilter = 10
      givenGraph {
        nodeIndex("Honey", "prop")
        nodePropertyGraph(
          n,
          {
            case i => Map("prop" -> i % modulo)
          },
          "Honey"
        )
      }

      val xValues = 0L until 2L
      val input = inputValues(xValues.map(Array[Any](_)): _*)

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "yprop", "zprop").withLeveragedOrder()
        .projection("y.prop AS yprop").withLeveragedOrder()
        .apply().withLeveragedOrder()
        .|.cartesianProduct().withLeveragedOrder()
        .|.|.sort("zprop DESC")
        .|.|.projection("z.prop AS zprop")
        .|.|.allNodeScan("z")
        .|.nodeIndexOperator(
          s"y:Honey(prop >= $zGreaterThanFilter)",
          indexOrder = indexOrder,
          getValue = _ => DoNotGetValue
        ).withProvidedOrder(providedOrderFactory("y.prop"))
        .input(variables = Seq("x"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime, input)

      // then
      val expected = for {
        x <- xValues
        y <- expectedMutation((0 until n).map(_ % modulo).filter(_ >= zGreaterThanFilter).sorted)
        z <- (0 until n).map(_ % modulo).sorted.reverse
      } yield Array(x, y, z)

      runtimeResult should beColumns("x", "yprop", "zprop").withRows(inOrder(expected))
    }

    test(s"aggregation keeps index provided $orderString order") {
      // given
      val n = sizeHint
      givenGraph {
        nodeIndex("Honey", "prop")
        nodePropertyGraph(
          n,
          {
            case i => Map("prop" -> i % 100)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop", "c").withLeveragedOrder()
        .aggregation(groupingExpressions = Seq("x.prop AS prop"), aggregationExpression = Seq("count(*) AS c"))
        .nodeIndexOperator(
          "x:Honey(prop >= 0)",
          indexOrder = indexOrder,
          getValue = _ => DoNotGetValue
        ).withProvidedOrder(providedOrderFactory("x.prop"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = expectedMutation(0 until 100).map(prop => Array(prop, n / 100))
      runtimeResult should beColumns("prop", "c").withRows(inOrder(expected))
    }

    test(s"node hash join keeps RHS index provided $orderString order") {
      // given
      val n = sizeHint / 2 // Expected to be a multiple of modulo
      val fillFactor = 3
      val modulo = 100
      val zGreaterThanFilter = 10

      val relTuples = (for (i <- 0 until n) yield {
        Seq.fill(fillFactor)((i, i, "SELF"))
      }).reduce(_ ++ _)

      givenGraph {
        nodeIndex("Honey", "prop")
        val nodes = nodePropertyGraph(
          n,
          {
            case i => Map("prop" -> i % modulo)
          },
          "Honey"
        )
        connect(nodes, relTuples)
        nodes
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop").withLeveragedOrder()
        .projection("y.prop AS prop")
        .nodeHashJoin("y")
        .|.expand("(z)-->(y)")
        .|.nodeIndexOperator(
          s"z:Honey(prop >= $zGreaterThanFilter)",
          indexOrder = indexOrder,
          getValue = _ => DoNotGetValue
        ).withProvidedOrder(providedOrderFactory("z.prop"))
        .expand("(x)-->(y)")
        .filter("x.prop % 2 = 0")
        .nodeByLabelScan("x", "Honey", IndexOrderNone)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val lhs = for {
        x <- (0 until n).map(_ % modulo).sorted if x % 2 == 0
        y <- Seq.fill(fillFactor)(x)
      } yield y
      val rhs = for {
        z <- expectedMutation((0 until n).map(_ % modulo).filter(_ >= zGreaterThanFilter).sorted)
        y <- Seq.fill(fillFactor)(z)
      } yield y
      val expected = for {
        rhs_y <-
          rhs.zipWithIndex.filter(_._2 % (n / modulo) == 0).map(
            _._1
          ) // Only every (n / modulo)th node (even though they have the same property) matches
        lhs_y <- lhs if lhs_y == rhs_y
      } yield lhs_y

      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"apply keeps LHS index provided $orderString order") {
      // given
      val n = sizeHint
      val fillFactor = 10
      val modulo = 100
      val zGreaterThanFilter = 10

      val relTuples = (for (i <- 0 until n) yield {
        Seq.fill(fillFactor)((i, i, "SELF"))
      }).reduce(_ ++ _)

      givenGraph {
        nodeIndex("Honey", "prop")
        val nodes = nodePropertyGraph(
          n,
          {
            case i => Map("prop" -> i % modulo)
          },
          "Honey"
        )
        connect(nodes, relTuples)
        nodes
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop").withLeveragedOrder()
        .projection("y.prop AS prop")
        .apply()
        .|.filter("y.prop % 2 = 0")
        .|.argument("y")
        .expand("(z)-->(y)")
        .nodeIndexOperator(
          s"z:Honey(prop >= $zGreaterThanFilter)",
          indexOrder = indexOrder,
          getValue = _ => DoNotGetValue
        ).withProvidedOrder(providedOrderFactory("z.prop"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = for {
        z <- expectedMutation((0 until n).map(_ % modulo).filter(_ >= zGreaterThanFilter).sorted)
        y <- Seq.fill(fillFactor)(z) if y % 2 == 0
      } yield y

      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"optional keeps index provided $orderString order") {
      // given
      val n = sizeHint
      val nInputNodes = 10
      val modulo = 100
      val zGTFilter = 50

      val nodes = givenGraph {
        nodeIndex("Honey", "prop")
        nodePropertyGraph(
          n,
          {
            case i => Map("prop" -> i % modulo)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "zProp").withLeveragedOrder()
        .projection("z.prop AS zProp")
        .apply()
        .|.optional("x")
        .|.filter("x.prop % 2 = 0").withProvidedOrder(providedOrderFactory("z.prop"))
        .|.nodeIndexOperator(
          s"z:Honey(prop >= $zGTFilter)",
          indexOrder = indexOrder,
          getValue = _ => DoNotGetValue,
          argumentIds = Set("x")
        ).withProvidedOrder(providedOrderFactory("z.prop"))
        .input(Seq("x"))
        .build()

      val runtimeResult =
        execute(logicalQuery, runtime, input = inputValues(nodes.take(nInputNodes).map(Array[Any](_)): _*))

      // then
      val expected = for {
        x <- nodes.take(nInputNodes)
        zProp <-
          if ((x.getId % modulo) % 2 == 0) {
            expectedMutation((0 until n)
              .map(i => (nodes(i), i % modulo))
              .filter { case (_, i) => i >= zGTFilter }
              .sortBy { case (_, i) => i }
              .map { case (_, i) => i })
          } else {
            Seq(null)
          }
      } yield Array(x, zProp)

      runtimeResult should beColumns("x", "zProp").withRows(inOrder(expected))
    }

    test(s"Aggregation on RHS of Apply keeps $orderString order from Apply's LHS") {
      // Size set up to not match with any Morsel size
      val size = (2 * 3 * 4 * 5) + 1
      nodeIndex("Honey", "num")
      val nodes = givenGraph {
        nodePropertyGraph(
          size,
          {
            case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("num", "c").withLeveragedOrder()
        .projection("a.num AS num")
        .apply()
        .|.aggregation(Seq.empty, Seq("count(*) AS c"))
        .|.argument("a")
        .nodeIndexOperator("a:Honey(num >= 0)", indexOrder = indexOrder, getValue = _ => GetValue).withProvidedOrder(
          providedOrderFactory("a.num")
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      val expected = expectedMutation(nodes.map(node => Array(node.getProperty("num"), 1)))

      runtimeResult should beColumns("num", "c").withRows(inOrder(expected))
    }

    test(s"Sort on RHS of Apply keeps $orderString order from Apply's LHS") {
      // Size set up to not match with any Morsel size
      val size = (2 * 3 * 4 * 5) + 1
      nodeIndex("Honey", "num")
      val nodes = givenGraph {
        nodePropertyGraph(
          size,
          {
            case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("num", "name").withLeveragedOrder()
        .projection("a.num AS num")
        .apply()
        .|.sort("name ASC")
        .|.projection("a.name AS name")
        .|.argument("a")
        .nodeIndexOperator("a:Honey(num >= 0)", indexOrder = indexOrder, getValue = _ => GetValue).withProvidedOrder(
          providedOrderFactory("a.num")
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      val expected = expectedMutation(nodes.map(node => {
        val i = node.getProperty("num")
        Array(i, s"bob${i.asInstanceOf[Int] % 10}")
      }))

      runtimeResult should beColumns("num", "name").withRows(inOrder(expected))
    }

    test(s"Top on RHS of Apply keeps $orderString order from Apply's LHS") {
      // Size set up to not match with any Morsel size
      val size = (2 * 3 * 4 * 5) + 1
      nodeIndex("Honey", "num")
      val nodes = givenGraph {
        nodePropertyGraph(
          size,
          {
            case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("num", "name").withLeveragedOrder()
        .projection("a.num AS num")
        .apply()
        .|.top(1, "name ASC")
        .|.projection("a.name AS name")
        .|.argument("a")
        .nodeIndexOperator("a:Honey(num >= 0)", indexOrder = indexOrder, getValue = _ => GetValue).withProvidedOrder(
          providedOrderFactory("a.num")
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      val expected = expectedMutation(nodes.map(node => {
        val i = node.getProperty("num")
        Array(i, s"bob${i.asInstanceOf[Int] % 10}")
      }))

      runtimeResult should beColumns("num", "name").withRows(inOrder(expected))
    }

    // This seems to work since AttachBuffer creates views of size 1.
    // And the the RHS of MrBuff has always only one complete controller, which
    // happens to be the next one in ArgumentRowId order.
    test(s"Cartesian Product on RHS of Apply keeps $orderString order from Apply's LHS") {
      // Size set up to not match with any Morsel size
      val size = (2 * 3 * 4 * 5) + 1
      nodeIndex("Honey", "num")
      val nodes = givenGraph {
        nodePropertyGraph(
          size,
          {
            case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("num", "name").withLeveragedOrder()
        .projection("a.num AS num")
        .apply()
        .|.projection("a.name AS name")
        .|.cartesianProduct()
        .|.|.argument("a")
        .|.argument("a")
        .nodeIndexOperator("a:Honey(num >= 0)", indexOrder = indexOrder, getValue = _ => GetValue).withProvidedOrder(
          providedOrderFactory("a.num")
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      val expected = expectedMutation(nodes.map(node => {
        val i = node.getProperty("num")
        Array(i, s"bob${i.asInstanceOf[Int] % 10}")
      }))

      runtimeResult should beColumns("num", "name").withRows(inOrder(expected))
    }

    test(s"Hash join on RHS of Apply keeps $orderString order from Apply's LHS") {
      // Size set up to not match with any Morsel size
      val size = (2 * 3 * 4 * 5) + 1
      nodeIndex("Honey", "num")
      val nodes = givenGraph {
        nodePropertyGraph(
          size,
          {
            case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("num", "name").withLeveragedOrder()
        .projection("a.num AS num")
        .apply()
        .|.projection("a.name AS name")
        .|.nodeHashJoin("a")
        .|.|.argument("a")
        .|.argument("a")
        .nodeIndexOperator("a:Honey(num >= 0)", indexOrder = indexOrder, getValue = _ => GetValue).withProvidedOrder(
          providedOrderFactory("a.num")
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      val expected = expectedMutation(nodes.map(node => {
        val i = node.getProperty("num")
        Array(i, s"bob${i.asInstanceOf[Int] % 10}")
      }))

      runtimeResult should beColumns("num", "name").withRows(inOrder(expected))
    }

    test(s"Optional on RHS of Apply keeps $orderString order from Apply's LHS") {
      // Size set up to not match with any Morsel size
      val size = (2 * 3 * 4 * 5) + 1
      nodeIndex("Honey", "num")
      val nodes = givenGraph {
        nodePropertyGraph(
          size,
          {
            case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("num", "name").withLeveragedOrder()
        .projection("a.num AS num")
        .apply()
        .|.optional("a")
        .|.projection("a.name AS name")
        .|.argument("a")
        .nodeIndexOperator("a:Honey(num >= 0)", indexOrder = indexOrder, getValue = _ => GetValue).withProvidedOrder(
          providedOrderFactory("a.num")
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      val expected = expectedMutation(nodes.map(node => {
        val i = node.getProperty("num")
        Array(i, s"bob${i.asInstanceOf[Int] % 10}")
      }))

      runtimeResult should beColumns("num", "name").withRows(inOrder(expected))
    }

    test(s"nested cartesian product keeps LHS and RHS provided $orderString order") {
      // given
      val n = sizeHint / 20
      val modulo = 20
      val zGreaterThanFilter = 10
      givenGraph {
        nodeIndex("Honey", "prop")
        nodePropertyGraph(
          n,
          {
            case i => Map("prop" -> i % modulo)
          },
          "Honey"
        )
      }

      val xValues = 0L until 7L
      val input = inputValues(xValues.map(Array[Any](_)): _*)

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "yprop", "zprop").withLeveragedOrder()
        .projection("y.prop AS yprop").withLeveragedOrder()
        .cartesianProduct().withLeveragedOrder()
        .|.cartesianProduct().withLeveragedOrder()
        .|.|.sort("zprop DESC")
        .|.|.projection("z.prop AS zprop")
        .|.|.allNodeScan("z")
        .|.nodeIndexOperator(
          s"y:Honey(prop >= $zGreaterThanFilter)",
          indexOrder = indexOrder,
          getValue = _ => DoNotGetValue
        ).withProvidedOrder(providedOrderFactory("y.prop"))
        .input(variables = Seq("x"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime, input)

      // then
      val expected = for {
        x <- xValues
        y <- expectedMutation((0 until n).map(_ % modulo).filter(_ >= zGreaterThanFilter).sorted)
        z <- (0 until n).map(_ % modulo).sorted.reverse
      } yield Array(x, y, z)

      runtimeResult should beColumns("x", "yprop", "zprop").withRows(inOrder(expected))
    }
  }

  test("apply with non-grouping aggregation on the rhs should keep order when rows are filtered out") {
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.aggregation(Seq(), Seq("collect(y) as ys"))
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x > (rand() * $sizeHint)")
      .|.argument("x")
      .sort("x ASC")
      .unwind(s"range(0, $sizeHint) AS x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = Range.inclusive(0, sizeHint).map(i => Array(i))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("apply with grouping aggregation on the rhs should keep order when rows are filtered out") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.aggregation(Seq("x AS x"), Seq("collect(y) as ys"))
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(_ < 0.5).map(i => Array(i))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("anti conditional apply should keep order of lhs") {
    val input = Range(0, sizeHint).map { _ =>
      val x = randomValues.nextDouble()
      val y = if (randomValues.nextBoolean()) true else null
      (x, y)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .antiConditionalApply("y").withLeveragedOrder()
      .|.unwind("[0] AS z") // Pipeline break
      .|.filter(s"x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x", "y"))
      .build()

    val inputIterator = input.iterator.map { case (x, y) => Array[Any](x, y) }
    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(inputIterator))

    val expected = input
      .filter { case (x, y) => x < 0.5 || y != null }
      .map { case (x, y) => Array(x, y) }
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("apply with anti conditional apply on the rhs should keep order of lhs") {
    val input = Range(0, sizeHint).map { _ =>
      val x = randomValues.nextDouble()
      val y = if (randomValues.nextBoolean()) true else null
      (x, y)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.antiConditionalApply("y").withLeveragedOrder()
      .|.|.unwind("[0] AS b") // Pipeline break
      .|.|.filter(s"x < 0.25")
      .|.|.argument("x")
      .|.unwind("[0] AS a") // Pipeline break
      .|.filter("x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x", "y"))
      .build()

    val inputIterator = input.iterator.map { case (x, y) => Array[Any](x, y) }
    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(inputIterator))

    val expected = input
      .filter { case (x, y) => x < 0.5 && (y != null || x < 0.25) }
      .map { case (x, y) => Array(x, y) }
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("anti semi apply should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x > 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filterNot(_ > 0.5).map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("apply with anti semi apply on rhs should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.antiSemiApply()
      .|.|.unwind("[0] AS b") // Pipeline break
      .|.|.filter(s"x > 0.25")
      .|.|.argument("x")
      .|.unwind("[0] AS a") // Pipeline break
      .|.filter("x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(v => v <= 0.25).map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("apply with cartesian product on the rhs should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())
    givenGraph {
      nodePropertyGraph(1, { case i => Map("prop" -> i) }, "A")
      nodePropertyGraph(1, { case i => Map("prop" -> i) }, "B")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "`a.prop`", "`b.prop`")
      .projection("a.prop AS `a.prop`", "b.prop AS `b.prop`")
      .apply()
      .|.cartesianProduct()
      .|.|.unwind("[0] AS z") // Pipeline break
      .|.|.filter("x > 0.5")
      .|.|.nodeByLabelScan("b", "B", "x")
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter("x > 0.25")
      .|.nodeByLabelScan("a", "A", "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(_ > 0.5).map(v => Array(v, 0, 0))
    runtimeResult should beColumns("x", "a.prop", "b.prop").withRows(inOrder(expected))
  }

  test("conditional apply should keep order of lhs") {
    val input = Range(0, sizeHint).map { _ =>
      val x = randomValues.nextDouble()
      val y = if (randomValues.nextBoolean()) true else null
      (x, y)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .conditionalApply("y").withLeveragedOrder()
      .|.unwind("[0] AS z") // Pipeline break
      .|.filter(s"x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x", "y"))
      .build()

    val inputIterator = input.iterator.map { case (x, y) => Array[Any](x, y) }
    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(inputIterator))

    val expected = input
      .filter { case (x, y) => x < 0.5 || y == null }
      .map { case (x, y) => Array(x, y) }
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("apply with conditional apply on the rhs should keep order of lhs") {
    val input = Range(0, sizeHint).map { _ =>
      val x = randomValues.nextDouble()
      val y = if (randomValues.nextBoolean()) true else null
      (x, y)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.conditionalApply("y").withLeveragedOrder()
      .|.|.unwind("[0] AS b") // Pipeline break
      .|.|.filter(s"x < 0.25")
      .|.|.argument("x")
      .|.unwind("[0] AS a") // Pipeline break
      .|.filter("x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x", "y"))
      .build()

    val inputIterator = input.iterator.map { case (x, y) => Array[Any](x, y) }
    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(inputIterator))

    val expected = input
      .filter { case (x, y) => x < 0.5 && (y == null || x < 0.25) }
      .map { case (x, y) => Array(x, y) }
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("apply with optional on rhs should keep order of lhs") {
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.optional("x")
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x > (rand() * $sizeHint)")
      .|.argument("x")
      .sort("x ASC")
      .unwind(s"range(0, $sizeHint) AS x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = Range.inclusive(0, sizeHint).map(i => Array(i))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  ignore("apply with ordered aggregation on the rhs should keep order") {
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.orderedAggregation(Seq("x"), Seq("collect(y) as ys"), Seq("y"))
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x > (rand() * $sizeHint)")
      .|.argument("x")
      .sort("x ASC")
      .unwind(s"range(0, $sizeHint) AS x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = Range.inclusive(0, sizeHint).map(i => Array(i))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("rollup apply should keep order from lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "rollup")
      .rollUpApply("rollup", "x")
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.map(x => Array[Any](x, if (x < 0.5) Array(x) else Array()))
    runtimeResult should beColumns("x", "rollup").withRows(inOrder(expected))
  }

  test("apply with rollup apply on the rhs should keep order from lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "rollup")
      .apply()
      .|.rollUpApply("rollup", "x")
      .|.|.unwind("[0] AS b") // Pipeline break
      .|.|.filter(s"x < 0.25")
      .|.|.argument("x")
      .|.unwind("[0] AS a") // Pipeline break
      .|.filter("x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input
      .filter(_ < 0.5)
      .map(x => Array[Any](x, if (x < 0.25) Array(x) else Array()))
    runtimeResult should beColumns("x", "rollup").withRows(inOrder(expected))
  }

  test("select or anti semi apply should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("x < 0.25").withLeveragedOrder()
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x < 0.75")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(v => v < 0.25 || v >= 0.75).map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("apply with select or anti semi apply on rhs should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.selectOrAntiSemiApply("x < 0.15").withLeveragedOrder()
      .|.|.unwind("[0] AS y") // Pipeline break
      .|.|.filter(s"x <= 0.35")
      .|.|.argument("x")
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter("x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input
      .filter(_ < 0.5)
      .filter(v => v < 0.15 || v > 0.35)
      .map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("select or semi apply should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrSemiApply("x < 0.25").withLeveragedOrder()
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x > 0.75")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(v => v < 0.25 || v > 0.75).map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("semi apply should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .semiApply()
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x > 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(_ > 0.5).map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("apply with semi apply on the rhs should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.semiApply()
      .|.|.unwind("[0] AS b") // Pipeline break
      .|.|.filter(s"x < 0.25")
      .|.|.argument("x")
      .|.unwind("[0] AS a") // Pipeline break
      .|.filter("x < 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(_ < 0.25).map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("apply with sort on rhs should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.sort("y ASC")
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter(s"x > 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(_ > 0.5).map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("apply with top with ties on rhs should keep order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.top1WithTies("y ASC")
      .|.unwind("[0] AS y")
      .|.filter(s"x > 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(_ > 0.5).map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("apply with top on rhs should not ruin order of lhs") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.top(1, "y ASC")
      .|.unwind("[rand(), rand()] AS y")
      .|.filter(s"x > 0.5")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(_ > 0.5).map(v => Array(v))
    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("value hash join should keep order from rhs") {
    givenGraph {
      nodeIndex("A", "prop")
      nodeIndex("B", "prop")
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) }, "A")
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) }, "B")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("`a.prop`", "`b.prop`")
      .projection("a.prop AS `a.prop`", "b.prop AS `b.prop`")
      .valueHashJoin("a.prop=b.prop")
      .|.nodeIndexOperator("b:B(prop >= 0)", indexOrder = IndexOrderDescending)
      .nodeIndexOperator("a:A(prop >= 0)", indexOrder = IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = Range(0, sizeHint).map(i => Array(i, i)).reverse
    runtimeResult should beColumns("a.prop", "b.prop").withRows(inOrder(expected))
  }

  test("apply with value hash join on the rhs should keep order when rows are filtered out") {
    val input = Range(0, sizeHint).map(_ => randomValues.nextDouble())
    givenGraph {
      nodePropertyGraph(1, { case i => Map("prop" -> i) }, "A")
      nodePropertyGraph(1, { case i => Map("prop" -> i) }, "B")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "`a.prop`", "`b.prop`")
      .projection("a.prop AS `a.prop`", "b.prop AS `b.prop`")
      .apply()
      .|.valueHashJoin("a.prop=b.prop")
      .|.|.unwind("[0] AS z") // Pipeline break
      .|.|.filter("x > 0.5")
      .|.|.nodeByLabelScan("b", "B", "x")
      .|.unwind("[0] AS y") // Pipeline break
      .|.filter("x > 0.25")
      .|.nodeByLabelScan("a", "A", "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, iteratorInput(input.iterator.map(v => Array[Any](v))))

    val expected = input.filter(_ > 0.5).map(v => Array(v, 0, 0))
    runtimeResult should beColumns("x", "a.prop", "b.prop").withRows(inOrder(expected))
  }
}

trait CartesianProductProvidedOrderTestBase[CONTEXT <: RuntimeContext] {
  self: ProvidedOrderTestBase[CONTEXT] =>

  private[this] val parse: String => Expression = Parser.parseExpression
  private[this] val asc: Expression => ProvidedOrder = ProvidedOrder.asc(_: Expression)
  private[this] val desc: Expression => ProvidedOrder = ProvidedOrder.desc(_: Expression)

  for (
    ProvidedOrderTest(orderString, indexOrder, providedOrderFactory, expectedMutation) <- Seq(
      ProvidedOrderTest(
        "ascending",
        IndexOrderAscending,
        parse andThen asc,
        new SeqMutator {
          override def apply[X](in: Seq[X]): Seq[X] = in
        }
      ),
      ProvidedOrderTest(
        "descending",
        IndexOrderDescending,
        parse andThen desc,
        new SeqMutator {
          override def apply[X](in: Seq[X]): Seq[X] = in.reverse
        }
      )
    )
  ) {
    test(s"cartesian product keeps LHS index provided $orderString order") {
      // given
      val n = sizeHint / 10
      val modulo = 100
      val zGreaterThanFilter = 10
      givenGraph {
        nodeIndex("Honey", "prop")
        nodePropertyGraph(
          n,
          {
            case i => Map("prop" -> i % modulo)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop").withLeveragedOrder()
        .projection("z.prop AS prop").withLeveragedOrder()
        .cartesianProduct().withLeveragedOrder()
        .|.allNodeScan("y")
        .nodeIndexOperator(
          s"z:Honey(prop >= $zGreaterThanFilter)",
          indexOrder = indexOrder,
          getValue = _ => DoNotGetValue
        ).withProvidedOrder(providedOrderFactory("z.prop"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = for {
        z <- expectedMutation((0 until n).map(_ % modulo).filter(_ >= zGreaterThanFilter).sorted)
        _ <- 0 until n
      } yield z

      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"cartesian product keeps LHS and RHS provided $orderString order") {
      // given
      val n = sizeHint / 10
      val modulo = 100
      val zGreaterThanFilter = 10
      givenGraph {
        nodeIndex("Honey", "prop")
        nodePropertyGraph(
          n,
          {
            case i => Map("prop" -> i % modulo)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop", "yprop").withLeveragedOrder()
        .projection("z.prop AS prop").withLeveragedOrder()
        .cartesianProduct().withLeveragedOrder()
        .|.sort("yprop DESC")
        .|.projection("y.prop AS yprop")
        .|.allNodeScan("y")
        .nodeIndexOperator(
          s"z:Honey(prop >= $zGreaterThanFilter)",
          indexOrder = indexOrder,
          getValue = _ => DoNotGetValue
        ).withProvidedOrder(providedOrderFactory("z.prop"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = for {
        z <- expectedMutation((0 until n).map(_ % modulo).filter(_ >= zGreaterThanFilter).sorted)
        y <- (0 until n).map(_ % modulo).sorted.reverse
      } yield Array(z, y)

      runtimeResult should beColumns("prop", "yprop").withRows(inOrder(expected))
    }
  }
}
