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
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.kernel.impl.util.ValueUtils

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

abstract class DeleteDetachExpressionTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("detach delete node in map") {
    val nodes = given {
      chainGraphs(3, "FEELS", "FEELS", "SEES")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("map")
      .detachDeleteExpression("map.node")
      .projection("{node: n} AS map")
      .allNodeScan("n")
      .build(readOnly = false)

    val expectedRows = nodes
      .flatMap(_.nodes().asScala)
      .map(n => ValueUtils.asMapValue(Map("node" -> n).asJava))
      .map(m => Array(m))
    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("map")
      .withRows(expectedRows)
      .withStatistics(nodesDeleted = 3 * 4, relationshipsDeleted = 3 * 3)
    Iterables.count(tx.getAllNodes) shouldBe 0
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }

  test("detach delete relationship in map") {
    val nodes = given {
      chainGraphs(3, "LOVES", "LOVES", "LOVES_TO_HATE")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("map")
      .detachDeleteExpression("map.relationship")
      .projection("{relationship: r} AS map")
      .expand("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    val expectedRows = nodes
      .flatMap(_.relationships().asScala)
      .map(n => ValueUtils.asMapValue(Map("relationship" -> n).asJava))
      .map(m => Array(m))

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("map")
      .withRows(expectedRows)
      .withStatistics(relationshipsDeleted = 3 * 3)
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }

  test("detach delete path in map") {
    given {
      chainGraphs(3, "LOVES", "LOVES", "LOVES_TO_HATE")
    }

    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        SingleRelationshipPathStep(varFor("r"), OUTGOING, None, NilPathStep()(pos))(pos)
      )(pos)
    )(pos)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("map")
      .detachDeleteExpression("map.path")
      .eager()
      .projection("{path: p} AS map")
      .projection(Map("p" -> pathExpression))
      .expand("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("map")
      .withStatistics(nodesDeleted = 3 * 4, relationshipsDeleted = 3 * 3)
    Iterables.count(tx.getAllNodes) shouldBe 0
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }
}
