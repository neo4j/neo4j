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

import scala.util.Random

abstract class NodeByIdSeekTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private val random = new Random(77)

  test("should find single node") {
    // given
    val nodes = nodeGraph(sizeHint)
    val toFind = nodes(random.nextInt(nodes.length))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", toFind.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(toFind)))
  }

  test("should not find non-existing node") {
    // given
    val toNotFind = nodeGraph(sizeHint).map(_.getId).max + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", toNotFind)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should find multiple nodes") {
    // given
    val nodes = nodeGraph(sizeHint)
    val toFind = (1 to 5).map(_ => nodes(random.nextInt(nodes.length)))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", toFind.map(_.getId):_*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(toFind))
  }

  test("should find some nodes and not others") {
    // given
    val nodes = nodeGraph(sizeHint)
    val toFind = (1 to 5).map(_ => nodes(random.nextInt(nodes.length)))
    val toNotFind1 = nodes.map(_.getId).max + 1
    val toNotFind2 = toNotFind1 + 1
    val nodesToLookFor = toNotFind1 +: toFind.map(_.getId) :+ toNotFind2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", nodesToLookFor:_*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(toFind))
  }


}
