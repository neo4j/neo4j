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
import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.logical.builder.Parser
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.{DoNotGetValue, IndexOrder, IndexOrderAscending, IndexOrderDescending}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class ProvidedOrderTestBase[CONTEXT <: RuntimeContext](
                                                                 edition: Edition[CONTEXT],
                                                                 runtime: CypherRuntime[CONTEXT],
                                                                 val sizeHint: Int
                                                               ) extends RuntimeTestSuite[CONTEXT](
  edition.copyWith(
    // Choosing a number that will align less well with HashTable sizes
    GraphDatabaseSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(3),
    GraphDatabaseSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(3)
  ), runtime) {

  trait SeqMutator { def apply[X](in: Seq[X]): Seq[X]}
  case class ProvidedOrderTest(orderString: String, indexOrder: IndexOrder, providedOrderFactory: String => ProvidedOrder, expectedMutation: SeqMutator)

  for (
    ProvidedOrderTest(orderString, indexOrder, providedOrderFactory, expectedMutation) <- Seq(
      ProvidedOrderTest("ascending", IndexOrderAscending, Parser.parseExpression _ andThen ProvidedOrder.asc,
        new SeqMutator {
          override def apply[X](in: Seq[X]): Seq[X] = in
        }),
      ProvidedOrderTest("descending", IndexOrderDescending, Parser.parseExpression _ andThen ProvidedOrder.desc,
        new SeqMutator {
          override def apply[X](in: Seq[X]): Seq[X] = in.reverse
        })
    )
  ) {

    test(s"expand keeps index provided $orderString order") {
      // given
      val n = sizeHint
      val relTuples = (for(i <- 0 until n) yield {
        Seq(
          (i, (2 * i) % n, "OTHER"),
          (i, (3 * i) % n, "OTHER"),
          (i, (4 * i) % n, "OTHER"),
          (i, (5 * i) % n, "OTHER"),
          (i, (i + 1) % n, "NEXT"),
          (i, i, "SELF")
        )
      }).reduce(_ ++ _)
      val nodes = given {
        index("Honey", "prop")
        val nodes = nodePropertyGraph(n, {
          case i if i % 10 == 0 => Map("prop" -> i)
        },"Honey")
        connect(nodes, relTuples)
        nodes
      }


      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("x.prop AS prop")
        .expand("(y)-->(z)") // 6x more rows
        .expand("(x)-->(y)") // 6x more rows
        .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", indexOrder = indexOrder, getValue = DoNotGetValue).withProvidedOrder(providedOrderFactory("x.prop"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = expectedMutation(nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > n / 2}.flatMap(n => Seq.fill(6 * 6)(n)).map(_._2))
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"aggregation keeps index provided $orderString order") {
      // given
      val n = sizeHint
      given {
        index("Honey", "prop")
        nodePropertyGraph(n, {
          case i => Map("prop" -> i % 100)
        }, "Honey")
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop", "c")
        .aggregation(groupingExpressions = Seq("x.prop AS prop"), aggregationExpression = Seq("count(*) AS c"))
        .nodeIndexOperator("x:Honey(prop >= 0)", indexOrder = indexOrder, getValue = DoNotGetValue).withProvidedOrder(providedOrderFactory("x.prop"))
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


      val relTuples = (for(i <- 0 until n) yield {
        Seq.fill(fillFactor)((i, i, "SELF"))
      }).reduce(_ ++ _)

      val nodes = given {
        index("Honey", "prop")
        val nodes = nodePropertyGraph(n, {
          case i => Map("prop" -> i % modulo)
        }, "Honey")
        connect(nodes, relTuples)
        nodes
      }


      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("y.prop AS prop")
        .nodeHashJoin("y")
        .|.expand("(z)-->(y)")
        .|.nodeIndexOperator(s"z:Honey(prop >= $zGreaterThanFilter)", indexOrder = indexOrder, getValue = DoNotGetValue).withProvidedOrder(providedOrderFactory("z.prop"))
        .expand("(x)-->(y)")
        .filter("x.prop % 2 = 0")
        .nodeByLabelScan("x", "Honey")
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
        rhs_y <- rhs.zipWithIndex.filter(_._2 % (n / modulo) == 0).map(_._1) // Only every (n / modulo)th node (even though they have the same property) matches
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

      val relTuples = (for(i <- 0 until n) yield {
        Seq.fill(fillFactor)((i, i, "SELF"))
      }).reduce(_ ++ _)

      val nodes = given {
        index("Honey", "prop")
        val nodes = nodePropertyGraph(n, {
          case i => Map("prop" -> i % modulo)
        }, "Honey")
        connect(nodes, relTuples)
        nodes
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("y.prop AS prop")
        .apply()
        .|.filter("y.prop % 2 = 0")
        .|.argument("y")
        .expand("(z)-->(y)")
        .nodeIndexOperator(s"z:Honey(prop >= $zGreaterThanFilter)", indexOrder = indexOrder, getValue = DoNotGetValue).withProvidedOrder(providedOrderFactory("z.prop"))
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

      val nodes = given {
        index("Honey", "prop")
        nodePropertyGraph(n, {
          case i => Map("prop" -> i % modulo)
        }, "Honey")
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "zProp")
        .projection("z.prop AS zProp")
        .apply()
        .|.optional("x")
        .|.filter("x.prop % 2 = 0").withProvidedOrder(providedOrderFactory("z.prop"))
        .|.nodeIndexOperator(s"z:Honey(prop >= $zGTFilter)", indexOrder = indexOrder, getValue = DoNotGetValue, argumentIds = Set("x")).withProvidedOrder(providedOrderFactory("z.prop"))
        .input(Seq("x"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime, input = inputValues(nodes.take(nInputNodes).map(Array[Any](_)):_*))

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

    // This seems to work since AttachBuffer creates views of size 1.
    // And the the RHS of MrBuff has always only one complete controller, which
    // happens to be the next one in ArgumentRowId order.
    test(s"Cartesian Product on RHS of Apply keeps $orderString order from Apply's LHS") {
      // This size is specifically chosen to potentially break with Morsel size 3
      val size = 121
      index("Honey", "num")
      val nodes = given {
        nodePropertyGraph(size, {
          case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
        }, "Honey")
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("num", "name")
        .projection("a.num AS num")
        .apply()
        .|.projection("a.name AS name")
        .|.cartesianProduct()
        .|.|.argument("a")
        .|.argument("a")
        .nodeIndexOperator("a:Honey(num >= 0)", indexOrder = indexOrder, getValue = GetValue).withProvidedOrder(providedOrderFactory("a.num"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      val expected = expectedMutation(nodes.map(node => {
        val i = node.getProperty("num")
        Array(i, s"bob${i.asInstanceOf[Int] % 10}")
      }))

      runtimeResult should beColumns("num", "name").withRows(inOrder(expected))
    }

    test(s"Hash join on RHS of Apply keeps $orderString order from Apply's LHS") {
      // This size is specifically chosen to potentially break with Morsel size 3
      val size = 121
      index("Honey", "num")
      val nodes = given {
        nodePropertyGraph(size, {
          case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
        }, "Honey")
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("num", "name")
        .projection("a.num AS num")
        .apply()
        .|.projection("a.name AS name")
        .|.nodeHashJoin("a")
        .|.|.argument("a")
        .|.argument("a")
        .nodeIndexOperator("a:Honey(num >= 0)", indexOrder = indexOrder, getValue = GetValue).withProvidedOrder(providedOrderFactory("a.num"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      val expected = expectedMutation(nodes.map(node => {
        val i = node.getProperty("num")
        Array(i, s"bob${i.asInstanceOf[Int] % 10}")
      }))

      runtimeResult should beColumns("num", "name").withRows(inOrder(expected))
    }

    test(s"Optional on RHS of Apply keeps $orderString order from Apply's LHS") {
      // This size is specifically chosen to potentially break with Morsel size 3
      val size = 121
      index("Honey", "num")
      val nodes = given {
        nodePropertyGraph(size, {
          case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
        }, "Honey")
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("num", "name")
        .projection("a.num AS num")
        .apply()
        .|.optional("a")
        .|.projection("a.name AS name")
        .|.argument("a")
        .nodeIndexOperator("a:Honey(num >= 0)", indexOrder = indexOrder, getValue = GetValue).withProvidedOrder(providedOrderFactory("a.num"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      val expected = expectedMutation(nodes.map(node => {
        val i = node.getProperty("num")
        Array(i, s"bob${i.asInstanceOf[Int] % 10}")
      }))

      runtimeResult should beColumns("num", "name").withRows(inOrder(expected))
    }
  }
}

// Supported by interpreted, slotted
trait CartesianProductProvidedOrderTestBase[CONTEXT <: RuntimeContext] {
  self: ProvidedOrderTestBase[CONTEXT] =>

  for (
    ProvidedOrderTest(orderString, indexOrder, providedOrderFactory, expectedMutation) <- Seq(
      ProvidedOrderTest("ascending", IndexOrderAscending, Parser.parseExpression _ andThen ProvidedOrder.asc,
        new SeqMutator {
          override def apply[X](in: Seq[X]): Seq[X] = in
        }),
      ProvidedOrderTest("descending", IndexOrderDescending, Parser.parseExpression _ andThen ProvidedOrder.desc,
        new SeqMutator {
          override def apply[X](in: Seq[X]): Seq[X] = in.reverse
        })
    )
  ) {
    test(s"cartesian product keeps LHS index provided $orderString order") {
      // given
      val n = sizeHint / 10
      val modulo = 100
      val zGreaterThanFilter = 10

      given {
        index("Honey", "prop")
        nodePropertyGraph(n, {
          case i => Map("prop" -> i % modulo)
        }, "Honey")
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("z.prop AS prop")
        .cartesianProduct()
        .|.allNodeScan("y")
        .nodeIndexOperator(s"z:Honey(prop >= $zGreaterThanFilter)", indexOrder = indexOrder, getValue = DoNotGetValue).withProvidedOrder(providedOrderFactory("z.prop"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = for {
        z <- expectedMutation((0 until n).map(_ % modulo).filter(_ >= zGreaterThanFilter).sorted)
        _ <- 0 until n
      } yield z

      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }
  }
}
