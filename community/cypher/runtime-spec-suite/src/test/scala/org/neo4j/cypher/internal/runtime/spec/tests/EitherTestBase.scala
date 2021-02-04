/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.internal.helpers.collection.Iterables

abstract class EitherTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {


  test("should create if lhs is empty") {
    // given an empty data base

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .eitherApply()
      .|.create(createNode("n"))
      .|.argument()
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("n").withSingleRow(Iterables.single(tx.getAllNodes)).withStatistics(nodesCreated = 1)
  }

  test("should not create if lhs is non-empty") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .eitherApply()
      .|.create(createNode("n"))
      .|.argument()
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("n").withRows(singleColumn(nodes)).withNoUpdates()
  }

  test("should create if lhs is empty on the rhs of apply") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.eitherApply()
      .|.|.create(createNode("n"))
      .|.|.argument()
      .|.allNodeScan("n")
      .input(variables = Seq("m"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime, inputValues(inputRows:_*))
    consume(runtimeResult)
    val node = Iterables.single(tx.getAllNodes)
    runtimeResult should beColumns("n").withRows(singleColumn(Seq.fill(sizeHint)(node))).withStatistics(nodesCreated = 1)
  }

  test("should not create if lhs is non-empty on the rhs of apply") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.eitherApply()
      .|.|.create(createNode("n"))
      .|.|.argument()
      .|.allNodeScan("n")
      .input(variables = Seq("m"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime, inputValues(Array(1), Array(2), Array(3)))
    consume(runtimeResult)
    runtimeResult should beColumns("n").withRows(singleColumn(nodes.flatMap(Seq.fill(3)(_)))).withNoUpdates()
  }
}
