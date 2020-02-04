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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.storable.RandomValues

abstract class UnionTestBase[CONTEXT <: RuntimeContext](
                                                         edition: Edition[CONTEXT],
                                                         runtime: CypherRuntime[CONTEXT],
                                                         sizeHint: Int
                                                        ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should union single variable") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.projection("1 AS x")
      .|.argument()
      .projection("'hi' AS x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(1, "hi")))
  }

  test("should union single node variable") {
    // given
    val nodes = nodeGraph(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes ++ nodes))
  }

  test("should union node and non-node variable") {
    // given
    val nodes = nodeGraph(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .union()
      .|.projection("'right' AS y")
      .|.allNodeScan("x")
      .projection("'left' AS y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(
      nodes.map(n => Array(n, "left")) ++
      nodes.map(n => Array(n, "right"))
    )
  }

  test("should union node and relationship variables") {
    // given
    val (_, rels) = circleGraph(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r")
      .union()
      .|.expand("(x)-[r]->()")
      .|.allNodeScan("x")
      .expand("(x)<-[r]-()")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r").withRows(
      rels.map(r => Array(r.getStartNode, r)) ++
      rels.map(r => Array(r.getEndNode, r))
    )
  }

  test("should union nodes with other values") {
    // given
    val nodes = nodeGraph(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.allNodeScan("x")
      .input(variables = Seq("x"))
      .build()

    val random = RandomValues.create()
    val input = inputValues((0 until sizeHint).map(_ => Array[Any](random.nextValue())):_*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(nodes.map(Array(_)) ++ input.flatten)
  }

  test("should union many variables") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .union()
      .|.projection("a+10 AS x", "a+20 AS y", "a+30 AS z")
      .|.unwind("[1,2,3,4,5,6,7,8,9] AS a")
      .|.argument()
      .projection("'hi' AS x", "'ho' AS y", "'humbug' AS z")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(
      (1 to 9).map(a => Array[Any](a+10, a+20, a+30)) :+
        Array("hi", "ho", "humbug")
    )
  }

  test("should union many variables in permuted order") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .union()
      .|.projection("a+10 AS x", "a+20 AS y", "a+30 AS z")
      .|.unwind("[1,2,3,4,5,6,7,8,9] AS a")
      .|.argument()
      .projection("'ho' AS y", "'hi' AS x", "'humbug' AS z")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(
      (1 to 9).map(a => Array[Any](a+10, a+20, a+30)) :+
        Array("hi", "ho", "humbug")
    )
  }

  test("should union cached properties") {
    // given
    nodePropertyGraph(sizeHint, { case i => Map("prop" -> i)})

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[x.prop] AS prop")
      .union()
      .|.cacheProperties("x.prop")
      .|.allNodeScan("x")
      .cacheProperties("x.prop")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn((0 until sizeHint) ++ (0 until sizeHint)))
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // projection
  }
}
