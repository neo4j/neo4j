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
import org.neo4j.function.Predicates
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipExpansion
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.LocalMemoryTracker

import scala.collection.compat.immutable.ArraySeq

class TwoWaySignpostTest extends CypherFunSuite {
  private val meter = MemoryMeter.builder.build
  private def deduplicatedSize(o: AnyRef*) = meter.measureDeep(o) - meter.measureDeep(ArraySeq.fill(o.size)(null))

  test("memory allocation on construction of node signpost") {
    val mt = new LocalMemoryTracker()
    val gs = new GlobalState(null, null, SearchMode.Unidirectional, EmptyMemoryTracker.INSTANCE, PPBFSHooks.NULL, 1)

    val s1 = new State(1, SlotOrName.none, Predicates.ALWAYS_TRUE_LONG, false, false)
    val s2 = new State(2, SlotOrName.none, Predicates.ALWAYS_TRUE_LONG, false, false)

    val prevNode = new NodeState(gs, 1, s1, 3)
    val forwardNode = new NodeState(gs, 1, s2, 3)

    val signpost = TwoWaySignpost.fromNodeJuxtaposition(mt, prevNode, forwardNode, 0)

    val actual = meter.measureDeep(signpost) - deduplicatedSize(prevNode, forwardNode)

    mt.estimatedHeapMemory() shouldBe actual
  }

  test("memory allocation on construction of rel signpost") {
    val mt = new LocalMemoryTracker()
    val gs = new GlobalState(null, null, SearchMode.Unidirectional, EmptyMemoryTracker.INSTANCE, PPBFSHooks.NULL, 1)

    val s1 = new State(1, SlotOrName.none, Predicates.ALWAYS_TRUE_LONG, false, false)
    val s2 = new State(2, SlotOrName.none, Predicates.ALWAYS_TRUE_LONG, false, false)

    val prevNode = new NodeState(gs, 1, s1, 3)
    val forwardNode = new NodeState(gs, 2, s2, 3)

    val re = new RelationshipExpansion(s1, Predicates.alwaysTrue(), null, Direction.BOTH, SlotOrName.none, s2)

    val signpost = TwoWaySignpost.fromRelExpansion(mt, prevNode, 1, forwardNode, re, 0)

    val actual = meter.measureDeep(signpost) - deduplicatedSize(prevNode, forwardNode, re)

    mt.estimatedHeapMemory() shouldBe actual
  }
}
