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

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class ValueHashJoinTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                runtime: CypherRuntime[CONTEXT],
                                                                sizeHint: Int
                                                              ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

    test("should support simple hash join between two identifiers") {
      // given
      val nodes = nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("a", "b")
        .valueHashJoin("a.prop=b.prop")
        .|.allNodeScan("b")
        .allNodeScan("a")
        .build()
      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = nodes.map(n => Array(n, n))
      runtimeResult should beColumns("a", "b").withRows(expected)
    }

  test("should handle additional data when joining on two identifiers") {
    // given
    val nodes = nodePropertyGraph(sizeHint, {
      case i => Map("prop" -> i, "otherProp" -> i)
    })

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("aProp", "bProp")
      .valueHashJoin("a.prop=b.prop")
      .|.projection("b.otherProp AS bProp")
      .|.allNodeScan("b")
      .projection("a.otherProp AS aProp")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (0 until sizeHint).map(n => Array(n, n))
    runtimeResult should beColumns("aProp", "bProp").withRows(expected)
  }

  test("should join on a cached-property") {
    // given
    val nodes = nodePropertyGraph(sizeHint, {
      case i => Map("prop" -> i)
    })

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .valueHashJoin("cache[a.prop]=cache[b.prop]")
      .|.filter("cache[b.prop] < 10")
      .|.allNodeScan("b")
      .filter("cache[a.prop] < 20")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n)). take(10)
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should handle multiple columns") {
    // given
    val nodes = nodePropertyGraph(sizeHint, {
      case i => Map("prop" -> i)
    })
    val relTuples = (for (i <- 0 until sizeHint) yield {
      Seq(
        (i, (i + 1) % sizeHint, "R")
        )
    }).reduce(_ ++ _)

    connect(nodes, relTuples)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .valueHashJoin("c.prop=e.prop")
      .|.expand("(e)-->(f)")
      .|.expand("(d)-->(e)")
      .|.allNodeScan("d")
      .expand("(b)-->(c)")
      .expand("(a)-->(b)")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n))
    runtimeResult should beColumns("c").withRows(expected)
  }
}
