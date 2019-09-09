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

import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class CartesianProductTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {


  test("handle cached properties and cartesian product on LHS of apply") {
    // given
    val nodes = nodePropertyGraph(sizeHint, {
      case i: Int => Map("prop" -> i)
    }, "Label")
    index("Label", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.cartesianProduct()
      .|.|.argument("n")
      .|.argument("n")
      .nodeIndexOperator("n:Label(prop)", getValue = GetValue)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withRows(singleColumn(nodes))
  }

  test("should handle multiple columns ") {
    // given
    val size = 10 //sizehint is a bit too big here
    val nodes = nodePropertyGraph(size, {
      case _ => Map("prop" -> "foo")
    })
    val relTuples = (for (i <- 0 until size) yield {
      Seq(
        (i, (i + 1) % size, "R")
        )
    }).reduce(_ ++ _)
    connect(nodes, relTuples)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .cartesianProduct()
      .|.expand("(e)-->(f)")
      .|.expand("(d)-->(e)")
      .|.allNodeScan("d")
      .expand("(b)-->(c)")
      .expand("(a)-->(b)")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(c => (1 to size).map(_ => Array(c)))
    runtimeResult should beColumns("c").withRows(expected)
  }

}
