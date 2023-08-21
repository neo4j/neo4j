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
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.graphdb.Label

abstract class DeleteTypeSafetyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("delete node fail to delete relationship") {

    given {
      chainGraphs(1, "NOT_A_NODE")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .deleteNode("r")
      .expand("(n)-[r]-()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }

  test("delete node fail to delete path") {
    given {
      tx.createNode(Label.label("PleaseKillMe"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .deleteNode("p")
      .projection(Map("p" -> PathExpression(NodePathStep(varFor("n"), NilPathStep()(pos))(pos))(InputPosition.NONE)))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }

  test("delete path fail to delete node") {

    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .deletePath("n")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }

  test("delete path fail to delete relationship") {

    given {
      chainGraphs(1, "SEES")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .deletePath("r")
      .expand("(n)-[r]-()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }

  test("delete relationship fail to delete node") {

    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .deleteRelationship("n")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }

  test("delete relationship fail to delete path") {

    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .deleteRelationship("n")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }

  test("detach delete node fail to delete relationship") {

    given {
      chainGraphs(1, "NOT_A_NODE")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .detachDeleteNode("r")
      .expand("(n)-[r]-()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }

  test("detach delete node fail to delete path") {
    given {
      tx.createNode()
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .detachDeleteNode("p")
      .projection(Map("p" -> PathExpression(NodePathStep(varFor("n"), NilPathStep()(pos))(pos))(InputPosition.NONE)))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }

  test("detach delete path fail to delete node") {

    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .detachDeletePath("n")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }

  test("detach delete path fail to delete relationship") {

    given {
      chainGraphs(1, "SEES")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .detachDeletePath("r")
      .expand("(n)-[r]-()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    a[CypherTypeException] should be thrownBy consume(runtimeResult)
  }
}
