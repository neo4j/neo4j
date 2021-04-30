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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class LockNodesTestBase[CONTEXT <: RuntimeContext](
                                                             edition: Edition[CONTEXT],
                                                             runtime: CypherRuntime[CONTEXT],
                                                             sizeHint: Int
                                                           ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should lock nodes") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .lockNodes("x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x")
      .withRows(nodes.map(Array(_)))
      .withLockedEntities(nodes.map(_.getId).toSet)
  }

  test("should lock nodes - with refslots") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("xRef")
      .lockNodes("xRef")
      .unwind("[x] as xRef")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("xRef")
      .withRows(nodes.map(Array(_)))
      .withLockedEntities(nodes.map(_.getId).toSet)
  }

  test("should ignore to lock null nodes") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .lockNodes("x")
      .injectValue("x", null)
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x")
      .withRows(Array(null) +: nodes.map(Array(_)))
      .withLockedEntities(nodes.map(_.getId).toSet)
  }

  test("should lock nodes under an apply") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.lockNodes("x")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x")
      .withRows(nodes.map(Array(_)))
      .withLockedEntities(nodes.map(_.getId).toSet)
  }

  test("should lock nodes on top of limit under an apply") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.lockNodes("x")
      .|.limit(1)
      .|.allNodeScan("x")
      .input(variables = Seq("v"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null)))
    runtimeResult should beColumns("x")
      .withRows(nodes.take(1).map(Array(_)))
      .withLockedEntities(nodes.take(1).map(_.getId).toSet)
  }
}
