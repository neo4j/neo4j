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
import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.storable.Values

abstract class LockNodesTestBase[CONTEXT <: RuntimeContext](
                                                             edition: Edition[CONTEXT],
                                                             runtime: CypherRuntime[CONTEXT],
                                                             sizeHint: Int
                                                           ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should lock nodes") {
    assume(runtime != InterpretedRuntime)
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .lockNodes("x")
      .allNodeScan("x")
      .build()

    val logicalProcedureQuery = new LogicalQueryBuilder(this)
      .produceResults("resourceId")
      .procedureCall("db.listLocks() YIELD resourceId")
      .argument()
      .build()

    // request one row and lock the node
    val runtimeResult = execute(logicalQuery, runtime)
    request(1, runtimeResult)

    val procedureResult = execute(logicalProcedureQuery, runtime)
    procedureResult shouldNot beColumns("resourceId").withNoRows()

    consume(runtimeResult)
    val procedureResult1 = execute(logicalProcedureQuery, runtime)
    procedureResult1 should beColumns("resourceId").withRows(nodes.map(node => Array(Values.longValue(node.getId))))
    restartTx()

    val procedureResult2 = execute(logicalProcedureQuery, runtime)
    procedureResult2 should beColumns("resourceId").withNoRows()
  }

  test("should lock nodes - with refslots") {
    assume(runtime != InterpretedRuntime)
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("xRef")
      .lockNodes("xRef")
      .unwind("[x] as xRef")
      .allNodeScan("x")
      .build()

    val logicalProcedureQuery = new LogicalQueryBuilder(this)
      .produceResults("resourceId")
      .procedureCall("db.listLocks() YIELD resourceId")
      .argument()
      .build()

    // request one row and lock the node
    val runtimeResult = execute(logicalQuery, runtime)
    request(1, runtimeResult)

    val procedureResult = execute(logicalProcedureQuery, runtime)
    procedureResult shouldNot beColumns("resourceId").withNoRows()

    consume(runtimeResult)
    val procedureResult1 = execute(logicalProcedureQuery, runtime)
    procedureResult1 should beColumns("resourceId").withRows(nodes.map(node => Array(Values.longValue(node.getId))))
    restartTx()

    val procedureResult2 = execute(logicalProcedureQuery, runtime)
    procedureResult2 should beColumns("resourceId").withNoRows()
  }

  test("should ignore to lock null nodes") {
    assume(runtime != InterpretedRuntime)
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .lockNodes("x")
      .injectValue("x", null)
      .allNodeScan("x")
      .build()

    val logicalProcedureQuery = new LogicalQueryBuilder(this)
      .produceResults("resourceId")
      .procedureCall("db.listLocks() YIELD resourceId")
      .argument()
      .build()

    // request one row and lock the node
    val runtimeResult = execute(logicalQuery, runtime)
    request(1, runtimeResult)

    val procedureResult = execute(logicalProcedureQuery, runtime)
    procedureResult shouldNot beColumns("resourceId").withNoRows()

    consume(runtimeResult)
    val procedureResult1 = execute(logicalProcedureQuery, runtime)
    procedureResult1 should beColumns("resourceId").withRows(nodes.map(node => Array(Values.longValue(node.getId))))
    restartTx()

    val procedureResult2 = execute(logicalProcedureQuery, runtime)
    procedureResult2 should beColumns("resourceId").withNoRows()
  }
}
