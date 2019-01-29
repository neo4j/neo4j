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
import org.neo4j.cypher.internal.v4_0.logical.plans.GetValue
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class NodeIndexScanTestBase[CONTEXT <: RuntimeContext](
                                                             edition: Edition[CONTEXT],
                                                             runtime: CypherRuntime[CONTEXT],
                                                             sizeHint: Int
                                                           ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should scan all nodes of an index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(graphDb)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0}.map(_._1)
    runtimeResult should beColumns("x").withSingleValueRows(expected)
  }

  test("should scan all nodes of a unique index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    uniqueIndex("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(graphDb)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0}.map(_._1)
    runtimeResult should beColumns("x").withSingleValueRows(expected)
  }

  test("should cache properties") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(graphDb)
      .produceResults("x", "prop")
      .projection("cached[x.prop] AS prop")
      .nodeIndexOperator("x:Honey(prop)", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect{ case (n, i) if (i % 10) == 0 => Array(n, i)}
    runtimeResult should beColumns("x", "prop").withRows(expected)
  }

  test("should cache properties with a unique index") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    uniqueIndex("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(graphDb)
      .produceResults("x", "prop")
      .projection("cached[x.prop] AS prop")
      .nodeIndexOperator("x:Honey(prop)", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect{ case (n, i) if (i % 10) == 0 => Array(n, i)}
    runtimeResult should beColumns("x", "prop").withRows(expected)
  }
}
