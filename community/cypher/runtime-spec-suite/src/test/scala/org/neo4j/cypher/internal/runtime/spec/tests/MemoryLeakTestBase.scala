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
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.PruningVarLengthExpandFuzzTestBase.REL
import org.neo4j.graphdb.Node

import scala.util.Random

abstract class MemoryLeakTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {
  private val population: Int = 1000
  private val seed = 1669216213333L // System.currentTimeMillis()
  private val random = new Random(seed)

  test("var-expand should not leak memory") {
    // given
    val (nodes, _) = given(circleGraph(1000))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .expand(s"(from)-[*1..4]-(to)")
      .input(nodes = Seq("from"))
      .build()
    consume(execute(logicalQuery, runtime, inputValues(Array(nodes.head))))

    // then
    tx.kernelTransaction().memoryTracker().estimatedHeapMemory() shouldBe 0
  }

  test("pruning-var-expand should not leak memory") {
    // given
    val (nodes, _) = given(circleGraph(1000))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .pruningVarExpand(s"(from)-[*1..4]-(to)")
      .input(nodes = Seq("from"))
      .build()

    consume(execute(logicalQuery, runtime, inputValues(Array(nodes.head))))

    // then
    tx.kernelTransaction().memoryTracker().estimatedHeapMemory() shouldBe 0
  }

  test("bfs-pruning-var-expand should not leak memory") {
    // given
    val (nodes, _) = given(circleGraph(1000))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .bfsPruningVarExpand(s"(from)-[*1..4]-(to)")
      .input(nodes = Seq("from"))
      .build()

    consume(execute(logicalQuery, runtime, inputValues(Array(nodes.head))))

    // then
    tx.kernelTransaction().memoryTracker().estimatedHeapMemory() shouldBe 0
  }
}
