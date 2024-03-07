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

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer.TracedPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TestGraph.Rel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventPPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.AddTarget
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.NextLevel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor.DataGraphRelationshipCursor
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipPredicate
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.storageengine.api.RelationshipDirection
import org.neo4j.storageengine.api.RelationshipSelection

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.IteratorHasAsScala

class PGPathPropagatingBFSTest extends CypherFunSuite {

  /*************************************
   * simple tests on the nfa traversal *
   *************************************/

  test("single node is yielded when state is both start and final state") {
    val graph = TestGraph.builder
    val n = graph.node()

    val paths = fixture()
      .withGraph(graph.build())
      .from(n)
      .withNfa { sb =>
        sb.newState(true, true)
      }
      .paths()

    paths shouldBe Seq(Seq(n))
  }

  test("single node is yielded when there is an unconditional node juxtaposition between start and final state") {
    val graph = TestGraph.builder
    val n = graph.node()

    val paths = fixture()
      .withGraph(graph.build())
      .from(n)
      .withNfa { sb =>
        sb.newStartState().addNodeJuxtaposition(sb.newFinalState())
      }
      .paths()

    paths shouldBe Seq(Seq(n, n))
  }

  test("single node is omitted when there is an unmet conditional node juxtaposition between start and final state") {
    val graph = TestGraph.builder
    val n = graph.node()

    val paths = fixture()
      .withGraph(graph.build())
      .from(n)
      .withNfa { sb =>
        val e = sb.newFinalState()
        sb.newStartState().addNodeJuxtaposition(e, (_: Long) => false)
      }
      .paths()

    paths shouldBe Seq.empty
  }

  test(
    "single node with loop is yielded when there is an unconditional relationship expansion between start and final state"
  ) {
    val graph = TestGraph.builder
    val n = graph.node()
    val r = graph.rel(n, n)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n)
      .withNfa { sb =>
        sb.newStartState().addRelationshipExpansion(sb.newFinalState())
      }
      .paths()

    paths shouldBe Seq(Seq(n, r, n))
  }

  test(
    "two nodes and their relationship are yielded when there is an unconditional relationship expansion between start and final state"
  ) {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val r = graph.rel(n0, n1)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        sb.newStartState().addRelationshipExpansion(sb.newFinalState())
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n1))
  }

  test(
    "relationship expansions can traverse a relationship in the inverse direction"
  ) {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val r = graph.rel(n1, n0)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        sb.newStartState().addRelationshipExpansion(sb.newFinalState())
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n1))
  }

  test("relationship expansions can traverse a loop") {
    val graph = TestGraph.builder
    val a = graph.node()
    val r = graph.rel(a, a)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        sb.newStartState().addRelationshipExpansion(sb.newFinalState(), Direction.OUTGOING)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(Seq(a, r, a))
  }

  test("multiple discrete paths between two paths can be yielded") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val r1 = graph.rel(n0, n1)
    val r2 = graph.rel(n0, n1)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        sb.newStartState().addRelationshipExpansion(sb.newFinalState())
      }
      .paths()

    paths should contain theSameElementsAs Seq(
      Seq(n0, r1, n1),
      Seq(n0, r2, n1)
    )
  }

  test("relationship expansions may be filtered on the destination node") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n0, n1)
    val r = graph.rel(n0, n2)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newFinalState()
        sb.newStartState().addRelationshipExpansion(e, (n: Long) => n == n2)
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may be filtered on the relationship id") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n0, n1)
    val r = graph.rel(n0, n2)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newFinalState()
        sb.newStartState().addRelationshipExpansion(e, RelationshipPredicate.onId(_ == r))
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may be filtered on the relationship type") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n0, n1, 99)
    val r = graph.rel(n0, n2, 66)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newFinalState()
        sb.newStartState().addRelationshipExpansion(e, RelationshipPredicate.onType(_ == 66))
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may expose an array of accepted types") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n0, n1, 99)
    val r = graph.rel(n0, n2, 66)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newFinalState()
        sb.newStartState().addRelationshipExpansion(e, Array(66))
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may be filtered on INCOMING direction") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n0, n1)
    val r = graph.rel(n2, n0)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newFinalState()
        sb.newStartState().addRelationshipExpansion(e, Direction.INCOMING)
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may be filtered on OUTGOING direction") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n1, n0)
    val r = graph.rel(n0, n2)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newFinalState()
        sb.newStartState().addRelationshipExpansion(e, Direction.OUTGOING)
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may be filtered on the source node of the relationship") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n0, n1)
    val r = graph.rel(n2, n0)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newFinalState()
        sb.newStartState().addRelationshipExpansion(e, RelationshipPredicate.onSource(_ == n2))
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may be filtered on the target node of the relationship") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n1, n0)
    val r = graph.rel(n0, n2)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newFinalState()
        sb.newStartState().addRelationshipExpansion(e, RelationshipPredicate.onTarget(_ == n2))
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("two-hop path") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    val r0 = graph.rel(n1, n0)
    val r1 = graph.rel(n1, n2)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val s = sb.newState()
        sb.newStartState().addRelationshipExpansion(s)
        s.addRelationshipExpansion(sb.newFinalState())
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r0, n1, r1, n2))
  }

  /********************************
   * More complex NFAs and graphs *
   ********************************/

  test("paths of different lengths are yielded in order of length") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val cd = graph.rel(c, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(anyDirectedPath)
      .paths()

    paths shouldBe Seq(
      Seq(a),
      Seq(a, ab, b),
      Seq(a, ab, b, bc, c),
      Seq(a, ab, b, bc, c, cd, d)
    )
  }

  test("relationships are not traversed twice in the same direction") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)
    val ba = graph.rel(b, a)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newStartState()
        val a = sb.newState()
        val e = sb.newFinalState()

        s.addNodeJuxtaposition(a)
        a.addRelationshipExpansion(e, Direction.OUTGOING)
        e.addNodeJuxtaposition(a)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, a, ab, b),
      Seq(a, a, ab, b, b, ba, a)
    )
  }

  test("relationships are not traversed twice in the opposite direction") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newStartState()
        val a = sb.newState()
        val e = sb.newFinalState()

        s.addNodeJuxtaposition(a)
        a.addRelationshipExpansion(e)
        e.addNodeJuxtaposition(a)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, a, ab, b)
    )
  }

  test("loops are traversed in any order") {
    val graph = TestGraph.builder
    val a = graph.node()

    val r1 = graph.rel(a, a)
    val r2 = graph.rel(a, a)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newStartState("s")
        val a = sb.newState("a")
        val b = sb.newState("b")
        val e = sb.newFinalState("e")

        s.addNodeJuxtaposition(a)
        a.addRelationshipExpansion(b)
        b.addNodeJuxtaposition(a)
        b.addNodeJuxtaposition(e)
      }
      .from(a)
      .paths()

    paths should contain theSameElementsAs Seq(
      Seq(a, a, r1, a, a),
      Seq(a, a, r2, a, a),
      Seq(a, a, r1, a, a, r2, a, a),
      Seq(a, a, r2, a, a, r1, a, a)
    )
  }

  test("an r* pattern should be able to be represented by a two NFA states with NJ then loop RE") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val cd = graph.rel(c, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newStartState("s")
        val e = sb.newFinalState("e")
        s.addNodeJuxtaposition(e)
        e.addRelationshipExpansion(e)
      }
      .from(a)
      .logged
      .paths()

    paths shouldBe Seq(
      Seq(a, a),
      Seq(a, a, ab, b),
      Seq(a, a, ab, b, bc, c),
      Seq(a, a, ab, b, bc, c, cd, d)
    )
  }

  test("an r+ pattern should be able to be represented by two NFA states with an RE and loop RE") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val cd = graph.rel(c, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newStartState("s")
        val e = sb.newFinalState("e")

        s.addRelationshipExpansion(e)
        e.addRelationshipExpansion(e)
      }
      .from(a)
      .logged
      .paths()

    paths shouldBe Seq(
      Seq(a, ab, b),
      Seq(a, ab, b, bc, c),
      Seq(a, ab, b, bc, c, cd, d)
    )
  }

  test("an r+ pattern should be able to be represented by three NFA states with an initial NJ") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val cd = graph.rel(c, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newStartState("s")
        val a = sb.newState("a")
        val e = sb.newFinalState("e")

        s.addNodeJuxtaposition(a)
        a.addRelationshipExpansion(e)
        e.addNodeJuxtaposition(a)
      }
      .from(a)
      .logged
      .paths()

    paths shouldBe Seq(
      Seq(a, a, ab, b),
      Seq(a, a, ab, b, b, bc, c),
      Seq(a, a, ab, b, b, bc, c, c, cd, d)
    )
  }

  /***************************************************
   * grouping, K limit, into (early exit), filtering *
   ***************************************************/

  test("results are limited to K") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val r0 = graph.rel(n0, n1)
    val r1 = graph.rel(n0, n1)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa(singleRelPath)
      .withK(1)
      .paths()

    paths should (be(Seq(Seq(n0, r0, n1))) or be(Seq(Seq(n0, r1, n1))))
  }

  test("results are limited to K per source-target pair") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val ab1 = graph.rel(a, b)
    val ab2 = graph.rel(a, b)
    val ac1 = graph.rel(a, c)
    val ac2 = graph.rel(a, c)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(singleRelPath)
      .withK(1)
      .paths()

    paths should have length 2
    paths should (contain(Seq(a, ab1, b)) or contain(Seq(a, ab2, b)))
    paths should (contain(Seq(a, ac1, c)) or contain(Seq(a, ac2, c)))
  }

  test("results are limited to K groups when grouping is enabled") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    graph.rel(b, c)
    val cd = graph.rel(c, d)

    val ac = graph.rel(a, c)
    val bd = graph.rel(b, d)

    val ad = graph.rel(a, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val start = sb.newStartState()
        val a = sb.newState()
        val end = sb.newFinalState()
        start.addRelationshipExpansion(a, Direction.OUTGOING)
        a.addNodeJuxtaposition(start)
        a.addNodeJuxtaposition(end, (n: Long) => n == d)
      }
      .grouped
      .withK(2)
      .paths()

    paths should contain theSameElementsAs Seq(
      Seq(a, ad, d, d),
      Seq(a, ac, c, c, cd, d, d),
      Seq(a, ab, b, b, bd, d, d)
    )
  }

  test(
    "results are limited to K groups when grouping is enabled and the final element of a group is rejected from the filter"
  ) {
    // this tests a very specific interaction in the ppbfs iterator - see https://trello.com/c/G4MEDqwE/
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    graph.rel(a, b)
    graph.rel(b, c)
    graph.rel(c, d)

    graph.rel(a, c)
    graph.rel(b, d)

    graph.rel(a, d)

    val onlyFirstOfGroup = {
      var passed = Set.empty[Int]
      (length: Int) =>
        if (!passed.contains(length)) {
          passed = passed + length
          true
        } else false
    }

    val lengths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(anyDirectedPath)
      .project(pathLength)
      .filter(onlyFirstOfGroup)
      .into(d)
      .grouped
      .withK(2)
      .toList

    // prior to the bugfix, this would have yielded Seq(1, 2, 3)
    lengths shouldBe Seq(1, 2)
  }

  test("intoTarget does not yield paths to other targets that could be valid") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val ab = graph.rel(a, b)
    graph.rel(a, c)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(singleRelPath)
      .into(b)
      .paths()

    paths shouldBe Seq(Seq(a, ab, b))
  }

  test("non-inlined prefilter applies before K limit") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(anyDirectedPath)
      .filter(p => pathLength(p) > 1)
      .withK(1)
      .paths()

    paths shouldBe Seq(Seq(a, ab, b, bc, c))
  }

  test("non-inlined prefilter applies after projection") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    graph.rel(a, b)
    graph.rel(b, c)

    val lengths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(anyDirectedPath)
      .project(pathLength)
      .filter(_ > 0)
      .toList

    lengths shouldBe Seq(1, 2)
  }

  /*******************************************
   * Algorithm introspection via event hooks *
   *******************************************/

  test("intoTarget stops searching the graph after the target is saturated") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    graph.rel(a, b)
    graph.rel(b, c)

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(b)
      .withK(1)
      .withNfa(anyDirectedPath)
      .events()

    events should (contain(NextLevel(1)) and not contain NextLevel(2))
  }

  test("propagation is performed when a previously-seen node is encountered") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val ad = graph.rel(a, d)
    val db = graph.rel(d, b)

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(c)
      .withNfa(anyDirectedPath)
      .events()

    val expected = new EventRecorder()
      .nextLevel(2)
      .schedulePropagation(b, 2, 1)
      .returnPath(a, ab, b, bc, c)
      .nextLevel(3)
      .propagateLengthPair(b, 2, 1)
      .returnPath(a, ad, d, db, b, bc, c)
      .nextLevel(4)
      .getEvents

    events should contain inOrderElementsOf expected
  }

  test("node should not be considered a target if it does not match the intoTarget specifier") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    graph.rel(a, b)
    graph.rel(a, c)

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(c)
      .withNfa(anyDirectedPath)
      .events()

    events should (contain(AddTarget(c)) and not contain AddTarget(b))
  }

  /*******************
   * Memory tracking *
   *******************/

  test("memory tracking") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    graph.rel(a, b)

    val mt = new LocalMemoryTracker()

    // ignore the memory tracker for path tracer for the purposes of this test
    def createPathTracer(_mt: MemoryTracker, hooks: PPBFSHooks): PathTracer =
      new PathTracer(EmptyMemoryTracker.INSTANCE, hooks)

    val iter = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(anyDirectedPath)
      .withMemoryTracker(mt)
      .build(createPathTracer)

    val heap1 = mt.estimatedHeapMemory()

    iter.next() // a

    val heap2 = mt.estimatedHeapMemory()
    heap1 should be < heap2

    iter.next() // b

    val heap3 = mt.estimatedHeapMemory()
    heap2 should be < heap3

    iter.close()

    val heap4 = mt.estimatedHeapMemory()
    heap4 shouldBe 0
  }

  /*********************************************************
   * Examples of NFAs which currently break the algorithm! *
   *********************************************************/

  ignore("single node with loop in graph, single state with loop in NFA") {
    val graph = TestGraph.builder
    val a = graph.node()
    val r1 = graph.rel(a, a)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", true, true)
        s.addRelationshipExpansion(s, Direction.OUTGOING)
      }
      .from(a)
      .logged
      .paths()

    paths shouldBe Seq(
      Seq(a),
      Seq(a, r1, a)
    )
  }

  ignore("an r* pattern should be able to be represented by a single NFA state with loop RE") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", true, true)
        s.addRelationshipExpansion(s)
      }
      .from(a)
      .logged
      .paths()

    paths shouldBe Seq(
      Seq(a),
      Seq(a, ab, b)
    )
  }

  ignore("an r* pattern should be able to be represented by a two NFA states with loop RE then NJ") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newStartState("s")
        s.addRelationshipExpansion(s)
        val e = sb.newFinalState("e")
        s.addNodeJuxtaposition(e)
      }
      .from(a)
      .logged
      .paths()

    paths shouldBe Seq(
      Seq(a),
      Seq(a, ab, b)
    )
  }

  ignore("an r+ pattern should be able to be represented by two NFA states with an RE and NJ") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newStartState("s")
        val e = sb.newFinalState("e")

        s.addRelationshipExpansion(e)
        e.addNodeJuxtaposition(s)
      }
      .from(a)
      .logged
      .paths()

    paths shouldBe Seq(
      Seq(a, ab, b)
    )
  }

  ignore("an r+ pattern should be able to be represented by three NFA states with a final NJ") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newStartState("s")
        val a = sb.newState("a")
        val e = sb.newFinalState("e")

        s.addRelationshipExpansion(a)
        a.addNodeJuxtaposition(s)
        a.addNodeJuxtaposition(e)
      }
      .from(a)
      .logged
      .paths()

    paths shouldBe Seq(
      Seq(a, ab, b)
    )
  }

  private def anyDirectedPath(sb: PGStateBuilder): Unit = {
    val s = sb.newState(true, true)
    s.addRelationshipExpansion(s, Direction.OUTGOING)
  }

  private def singleRelPath(sb: PGStateBuilder): Unit = {
    sb.newStartState().addRelationshipExpansion(sb.newFinalState())
  }

  private def pathLength(path: TracedPath) = path.entities().count(_.entityType() == EntityType.RELATIONSHIP)

  case class FixtureBuilder[A](
    graph: TestGraph,
    nfa: PGStateBuilder,
    source: Long,
    intoTarget: Long,
    isGroup: Boolean,
    k: Int,
    projection: TracedPath => A,
    predicate: A => Boolean,
    mt: MemoryTracker,
    hooks: PPBFSHooks
  ) {

    def withGraph(graph: TestGraph): FixtureBuilder[A] = copy(graph = graph)

    def withNfa(f: PGStateBuilder => Unit): FixtureBuilder[A] = copy(nfa = {
      val builder = new PGStateBuilder
      f(builder)
      builder
    })
    def from(source: Long): FixtureBuilder[A] = copy(source = source)
    def into(intoTarget: Long): FixtureBuilder[A] = copy(intoTarget = intoTarget)
    def grouped: FixtureBuilder[A] = copy(isGroup = true)
    def withK(k: Int): FixtureBuilder[A] = copy(k = k)
    def withMemoryTracker(mt: MemoryTracker): FixtureBuilder[A] = copy(mt = mt)

    /** NB: wipes any configured filter, since the iterated item type will change */
    def project[B](projection: TracedPath => B): FixtureBuilder[B] =
      FixtureBuilder[B](graph, nfa, source, intoTarget, isGroup, k, projection, _ => true, mt, hooks)
    def filter(predicate: A => Boolean): FixtureBuilder[A] = copy(predicate = predicate)

    def build(createPathTracer: (MemoryTracker, PPBFSHooks) => PathTracer = new PathTracer(_, _)) =
      new PGPathPropagatingBFS[A](
        source,
        intoTarget,
        nfa.getStart.state(),
        new MockGraphCursor(graph),
        createPathTracer(mt, hooks),
        projection(_),
        predicate(_),
        isGroup,
        k,
        nfa.stateCount(),
        mt,
        hooks
      )

    def toList: Seq[A] = build().asScala.toList

    def logged: FixtureBuilder[A] = copy(hooks = LoggingPPBFSHooks)

    /** Run the iterator with event hooks attached */
    def events(): Seq[EventRecorder.Event] = {
      val recorder = new EventRecorder
      // run the iterator
      copy(hooks = new EventPPBFSHooks(recorder)).toList
      recorder.getEvents
    }

    /** Run the iterator and extract the entity ids from the yielded paths.
     * Graph IDs are unique across nodes and relationships so there is no risk of inadvertent overlap when comparing. */
    def paths()(implicit ev: A =:= TracedPath): Seq[Seq[Long]] =
      build().asScala.map(_.entities().map(_.id()).toSeq).toSeq
  }

  private def fixture() = FixtureBuilder[TracedPath](
    TestGraph.empty,
    new PGStateBuilder,
    -1L,
    -1L,
    isGroup = false,
    Int.MaxValue,
    identity,
    _ => true,
    EmptyMemoryTracker.INSTANCE,
    PPBFSHooks.NULL
  )
}

case class TestGraph(nodes: Set[Long], rels: Set[TestGraph.Rel]) {
  def withNode(id: Long): TestGraph = copy(nodes = nodes + id)

  def withRel(id: Long, source: Long, target: Long, relType: Int): TestGraph = {
    assert(nodes.contains(source) && nodes.contains(target))
    copy(rels = rels + Rel(id, source, target, relType))
  }

  def rels(node: Long): Iterator[(Rel, RelationshipDirection)] =
    rels.iterator.flatMap { rel =>
      if (node == rel.source && node == rel.target) {
        Some(rel -> RelationshipDirection.LOOP)
      } else if (node == rel.source) {
        Some(rel -> RelationshipDirection.OUTGOING)
      } else if (node == rel.target) {
        Some(rel -> RelationshipDirection.INCOMING)
      } else None
    }
}

object TestGraph {

  case class Rel(id: Long, source: Long, target: Long, relType: Int) {

    def opposite(x: Long): Long =
      x match {
        case `target` => source
        case `source` => target
        case _        => throw new IllegalArgumentException()
      }
  }
  val empty: TestGraph = TestGraph(Set.empty, Set.empty)
  def builder = new Builder()

  class Builder {
    var nextId = 0L
    private var graph = TestGraph.empty

    def node(): Long = {
      nextId += 1
      graph = graph.withNode(nextId)
      nextId
    }

    def rel(source: Long, target: Long): Long = rel(source, target, 1)

    def rel(source: Long, target: Long, relType: Int): Long = {
      nextId += 1
      graph = graph.withRel(nextId, source, target, relType)
      nextId
    }

    def build(): TestGraph = graph
  }
}

class MockGraphCursor(graph: TestGraph) extends DataGraphRelationshipCursor {
  private var rels = Iterator.empty[Rel]
  private var currentNode: Long = -1L
  private var current: Rel = _

  def nextRelationship(): Boolean = {
    if (rels.hasNext) {
      current = rels.next()
      true
    } else {
      current = null
      false
    }
  }

  def setNode(node: Long, selection: RelationshipSelection): Unit = {
    currentNode = node
    rels = graph.rels(node)
      .collect { case (rel, dir) if selection.test(rel.relType, dir) => rel }
  }

  def relationshipReference(): Long = this.current.id

  def originNode(): Long = this.currentNode

  def otherNode(): Long = this.current.opposite(this.currentNode)

  def sourceNodeReference(): Long = this.current.source

  def targetNodeReference(): Long = this.current.target

  def `type`(): Int = this.current.relType

  def setTracer(tracer: KernelReadTracer): Unit = ()

  def source(cursor: NodeCursor): Unit = ???

  def target(cursor: NodeCursor): Unit = ???
}
