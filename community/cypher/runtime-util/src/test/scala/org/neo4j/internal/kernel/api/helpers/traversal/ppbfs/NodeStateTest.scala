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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs

import org.github.jamm.MemoryMeter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker

import scala.collection.compat.immutable.ArraySeq

class NodeStateTest extends CypherFunSuite {
  private val meter = MemoryMeter.builder.build
  private def deduplicatedSize(o: AnyRef*) = meter.measureDeep(o) - meter.measureDeep(ArraySeq.fill(o.size)(null))

  test("isTarget() returns true for a final state if there is no intoTarget") {
    val stateBuilder = new PGStateBuilder
    val state = stateBuilder.newState(isFinalState = true)
    val nodeData = new NodeState(globalState(), 1, state.state, NO_SUCH_NODE)

    nodeData.isTarget shouldBe true
  }

  test("isTarget() returns false if intoTarget does not match the node") {
    val stateBuilder = new PGStateBuilder
    val state = stateBuilder.newState(isFinalState = true)
    val nodeData = new NodeState(globalState(), 1, state.state, 2)

    nodeData.isTarget shouldBe false
  }

  test("isTarget() returns true if intoTarget matches the node") {
    val stateBuilder = new PGStateBuilder
    val state = stateBuilder.newState(isFinalState = true)
    val nodeData = new NodeState(globalState(), 1, state.state, 1)

    nodeData.isTarget shouldBe true
  }

  test("memory allocation on construction") {
    val mt = new LocalMemoryTracker()
    val state = new PGStateBuilder().newState().state
    val gs = globalState(mt)
    val ns = new NodeState(gs, 0, state, -1)

    val actual = meter.measureDeep(ns) - deduplicatedSize(gs, state)

    mt.estimatedHeapMemory() shouldBe actual
  }

  private def globalState(mt: MemoryTracker = EmptyMemoryTracker.INSTANCE) = {
    val hooks = PPBFSHooks.NULL
    new GlobalState(
      new Propagator(EmptyMemoryTracker.INSTANCE, hooks),
      new TargetTracker(EmptyMemoryTracker.INSTANCE, hooks),
      SearchMode.Unidirectional,
      mt,
      hooks,
      1
    )
  }
}
