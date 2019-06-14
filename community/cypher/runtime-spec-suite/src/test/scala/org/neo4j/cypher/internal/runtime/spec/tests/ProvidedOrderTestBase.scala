/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.logical.plans.{DoNotGetValue, IndexOrder, IndexOrderAscending, IndexOrderDescending}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class ProvidedOrderTestBase[CONTEXT <: RuntimeContext](
                                                                 edition: Edition[CONTEXT],
                                                                 runtime: CypherRuntime[CONTEXT],
                                                                 val sizeHint: Int
                                                               ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  trait SeqMutator { def apply[X](in: Seq[X]): Seq[X]}
  case class ProvidedOrderTest(orderString: String, indexOrder: IndexOrder, expectedMutation: SeqMutator)

  for(
    ProvidedOrderTest(orderString, indexOrder, expectedMutation) <- Seq(
      ProvidedOrderTest("ascending", IndexOrderAscending, new SeqMutator {
        override def apply[X](in: Seq[X]): Seq[X] = in
      }),
      ProvidedOrderTest("descending", IndexOrderDescending, new SeqMutator {
        override def apply[X](in: Seq[X]): Seq[X] = in.reverse
      })
    )
  ) {

    test(s"expand keeps index provided $orderString order") {
      // given
      val n = sizeHint
      val nodes = nodePropertyGraph(n, {
        case i if i % 10 == 0 => Map("prop" -> i)
      },"Honey")
      index("Honey", "prop")

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

      connect(nodes, relTuples)

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("x.prop AS prop")
        .expand("(y)-->(z)")
        .expand("(x)-->(y)")
        .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", indexOrder = indexOrder, getValue = DoNotGetValue)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = expectedMutation(nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > n / 2}.flatMap(n => Seq.fill(36)(n)).map(_._2))
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"aggregation keeps index provided $orderString order") {
      // given
      val n = sizeHint
      nodePropertyGraph(n, {
        case i => Map("prop" -> i % 100)
      },"Honey")
      index("Honey", "prop")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop", "c")
        .aggregation(groupingExpressions = Seq("x.prop AS prop"), aggregationExpression = Seq("count(*) AS c"))
        .nodeIndexOperator("x:Honey(prop >= 0)", indexOrder = indexOrder, getValue = DoNotGetValue)
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
      val zGTFilter = 10

      val nodes = nodePropertyGraph(n, {
        case i => Map("prop" -> i % modulo)
      },"Honey")
      index("Honey", "prop")

      val relTuples = (for(i <- 0 until n) yield {
        Seq.fill(fillFactor)((i, i, "SELF"))
      }).reduce(_ ++ _)

      connect(nodes, relTuples)

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("y.prop AS prop")
        .nodeHashJoin("y")
        .|.expand("(z)-->(y)")
        .|.nodeIndexOperator(s"z:Honey(prop >= $zGTFilter)", indexOrder = indexOrder, getValue = DoNotGetValue)
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
        z <- expectedMutation((0 until n).map(_ % modulo).filter(_ >= zGTFilter).sorted)
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
      val zGTFilter = 10

      val nodes = nodePropertyGraph(n, {
        case i => Map("prop" -> i % modulo)
      },"Honey")
      index("Honey", "prop")

      val relTuples = (for(i <- 0 until n) yield {
        Seq.fill(fillFactor)((i, i, "SELF"))
      }).reduce(_ ++ _)

      connect(nodes, relTuples)

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("y.prop AS prop")
        .apply()
        .|.filter("y.prop % 2 = 0")
        .|.argument("y")
        .expand("(z)-->(y)")
        .nodeIndexOperator(s"z:Honey(prop >= $zGTFilter)", indexOrder = indexOrder, getValue = DoNotGetValue)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = for {
        z <- expectedMutation((0 until n).map(_ % modulo).filter(_ >= zGTFilter).sorted)
        y <- Seq.fill(fillFactor)(z) if y % 2 == 0
      } yield y

      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }
  }
}
