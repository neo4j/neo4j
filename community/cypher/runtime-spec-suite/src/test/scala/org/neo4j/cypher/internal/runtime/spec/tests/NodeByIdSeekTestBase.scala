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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.util.Random

abstract class NodeByIdSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private val random = new Random(77)

  test("should find single node") {
    // given
    val nodes = given { nodeGraph(17) }
    val toFind = nodes(random.nextInt(nodes.length))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", Set.empty, toFind.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(toFind)))
  }

  test("should find by floating point") {
    // given
    val Seq(node) = given { nodeGraph(1) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", Set.empty, node.getId.toDouble)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(node)))
  }

  test("should not find non-existing node") {
    // given
    val toNotFind = given { nodeGraph(sizeHint) }.map(_.getId).max + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", Set.empty, toNotFind)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should find multiple nodes") {
    // given
    val nodes = given { nodeGraph(sizeHint) }
    val toFind = (1 to 5).map(_ => nodes(random.nextInt(nodes.length)))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", Set.empty, toFind.map(_.getId): _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(toFind))
  }

  test("should find some nodes and not others") {
    // given
    val nodes = given { nodeGraph(sizeHint) }
    val toFind = (1 to 5).map(_ => nodes(random.nextInt(nodes.length)))
    val toNotFind1 = nodes.map(_.getId).max + 1
    val toNotFind2 = toNotFind1 + 1
    val nodesToLookFor = toNotFind1 +: toFind.map(_.getId) :+ toNotFind2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", Set.empty, nodesToLookFor: _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(toFind))
  }

  test("should handle nodeById + filter") {
    // given
    val nodes = given { nodeGraph(sizeHint) }
    val toSeekFor = (1 to 5).map(_ => nodes(random.nextInt(nodes.length)))
    val toFind = toSeekFor(random.nextInt(toSeekFor.length))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter(s"id(x) = ${toFind.getId}")
      .nodeByIdSeek("x", Set.empty, toSeekFor.map(_.getId): _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(toFind)))
  }

  test("should work on rhs of apply") {
    // given
    val nodes = given { nodeGraph(10) }
    val toSeekFor = (1 to 5).map(_ => nodes(random.nextInt(nodes.length)))
    val toFind = toSeekFor(random.nextInt(toSeekFor.length))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "x")
      .apply()
      .|.filter(s"id(x) = ${toFind.getId}")
      .|.nodeByIdSeek("x", Set.empty, toSeekFor.map(_.getId): _*)
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedRows = nodes.map(n => Array(n, toFind))
    runtimeResult should beColumns("n", "x").withRows(expectedRows)
  }
}
