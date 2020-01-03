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

import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class NodeIndexScanTestBase[CONTEXT <: RuntimeContext](
                                                             edition: Edition[CONTEXT],
                                                             runtime: CypherRuntime[CONTEXT],
                                                             sizeHint: Int
                                                           ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should scan all nodes of an index with a property") {
    val nodes = given {
      index("Honey", "calories")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("calories" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(calories)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter{ _.hasProperty("calories") }
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should scan all nodes of a unique index with a property") {
    val nodes = given {
      uniqueIndex("Honey", "calories")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("calories" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(calories)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter{ _.hasProperty("calories") }
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should scan all nodes of an index with multiple properties") {
    val nodes = given {
      index("Honey", "calories", "taste")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("calories" -> i, "taste" -> i)
        case i if i % 5 == 0 => Map("calories" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(calories,taste)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter{ n => n.hasProperty("calories") && n.hasProperty("taste") }
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should cache properties") {
    val nodes = given {
      index("Honey", "calories")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("calories" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "calories")
      .projection("cache[x.calories] AS calories")
      .nodeIndexOperator("x:Honey(calories)", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect{ case (n, i) if n.hasProperty("calories") => Array(n, i)}
    runtimeResult should beColumns("x", "calories").withRows(expected)
  }

  test("should cache properties with a unique index") {
    val nodes = given {
      uniqueIndex("Honey", "calories")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("calories" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "calories")
      .projection("cache[x.calories] AS calories")
      .nodeIndexOperator("x:Honey(calories)", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect{ case (n, i) if n.hasProperty("calories") => Array(n, i)}
    runtimeResult should beColumns("x", "calories").withRows(expected)
  }
}
