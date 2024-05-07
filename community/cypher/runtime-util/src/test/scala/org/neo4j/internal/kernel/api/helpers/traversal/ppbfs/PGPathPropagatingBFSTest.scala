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
import org.neo4j.function.Predicates
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.RelationshipDataReader
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.OrderedResults
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.`(a)-->(b)-->(c)`
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.`(a)-->(b)<--(c)`
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.`(a)-->(b)`
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.`(a)<--(b)-->(a)`
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.`(a)<--(b)-->(c)`
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.`(s) ((a)--(b))+ (t)`
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.`(s) ((a)--(b)--(c))* (t)`
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.`(s) ((a)-->(b))* (t)`
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.`(s) ((a)-->(b))+ (t)`
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.anyDirectedPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.pathLength
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.singleRelPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest.testGraphs
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer.PathEntity
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer.TracedPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TestGraph.Rel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventPPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.AddTarget
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.Expand
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.ExpandNode
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.NextLevel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.ReturnPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor.DataGraphRelationshipCursor
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipPredicate
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State
import org.neo4j.kernel.api.AssertOpen
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.storageengine.api.RelationshipDirection
import org.neo4j.storageengine.api.RelationshipSelection

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
        sb.newState(isStartState = true, isFinalState = true)
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
        sb.newState(isStartState = true).addNodeJuxtaposition(sb.newState(isFinalState = true))
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
        val e = sb.newState(isFinalState = true, predicate = Predicates.ALWAYS_FALSE_LONG)
        sb.newState(isStartState = true).addNodeJuxtaposition(e)
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
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(n, r, n))
  }

  test(
    "two nodes and their relationship are yielded when there is an unconditional relationship expansion between start and final state"
  ) {
    val graph = `(a)-->(b)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.a)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.a, graph.ab, graph.b))
  }

  test(
    "relationship expansions can traverse a relationship in the inverse direction"
  ) {
    val graph = `(a)-->(b)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.ab, graph.a))
  }

  test("relationship expansions can traverse a loop") {
    val graph = TestGraph.builder
    val a = graph.node()
    val r = graph.rel(a, a)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(
          sb.newState(isFinalState = true),
          direction = Direction.OUTGOING
        )
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
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths should contain theSameElementsAs Seq(
      Seq(n0, r1, n1),
      Seq(n0, r2, n1)
    )
  }

  test("relationship expansions may be filtered on the destination node") {
    val graph = `(a)<--(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        sb.newState(isStartState = true)
          .addRelationshipExpansion(sb.newState(isFinalState = true, predicate = (n: Long) => n == graph.a))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.ba, graph.a))
  }

  test("relationship expansions may be filtered on the relationship id") {
    val graph = `(a)<--(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, RelationshipPredicate.onId(_ == graph.bc))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.bc, graph.c))
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
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, RelationshipPredicate.onType(_ == 66))
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
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, types = Array(66))
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may be filtered on INCOMING direction") {
    val graph = `(a)-->(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, direction = Direction.INCOMING)
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.ab, graph.a))
  }

  test("relationship expansions may be filtered on OUTGOING direction") {
    val graph = `(a)-->(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, direction = Direction.OUTGOING)
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.bc, graph.c))
  }

  test("relationship expansions may be filtered on the source node of the relationship") {
    val graph = `(a)-->(b)<--(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, RelationshipPredicate.onSource(_ == graph.a))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.ab, graph.a))
  }

  test("relationship expansions may be filtered on the target node of the relationship") {
    val graph = `(a)<--(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, RelationshipPredicate.onTarget(_ == graph.c))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.bc, graph.c))
  }

  test("two-hop path") {
    val graph = `(a)-->(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.a)
      .withNfa { sb =>
        val s = sb.newState()
        sb.newState(isStartState = true).addRelationshipExpansion(s)
        s.addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.a, graph.ab, graph.b, graph.bc, graph.c))
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
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, a, ab, b, b),
      Seq(a, a, ab, b, b, ba, a, a)
    )
  }

  test("relationships are not traversed again in the opposite direction") {
    val graph = `(a)-->(b)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.a)
      .withNfa(`(s) ((a)--(b))+ (t)`)
      .paths()

    paths shouldBe Seq(
      Seq(graph.a, graph.a, graph.ab, graph.b, graph.b)
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
      .withNfa(`(s) ((a)-->(b))+ (t)`)
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
        val s = sb.newState("s", isStartState = true)
        val e = sb.newState("e", isFinalState = true)
        s.addNodeJuxtaposition(e)
        e.addRelationshipExpansion(e)
      }
      .from(a)
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
        val s = sb.newState("s", isStartState = true)
        val e = sb.newState("e", isFinalState = true)

        s.addRelationshipExpansion(e)
        e.addRelationshipExpansion(e)
      }
      .from(a)
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
        val s = sb.newState("s", isStartState = true)
        val a = sb.newState("a")
        val e = sb.newState("e", isFinalState = true)

        s.addNodeJuxtaposition(a)
        a.addRelationshipExpansion(e)
        e.addNodeJuxtaposition(a)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, a, ab, b),
      Seq(a, a, ab, b, b, bc, c),
      Seq(a, a, ab, b, b, bc, c, c, cd, d)
    )
  }

  /**************************************************************
   * grouping, K limit, into (early exit), filtering, max depth *
   **************************************************************/

  test("results are limited to K") {
    val graph = `(a)<--(b)-->(a)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa(singleRelPath)
      .withK(1)
      .paths()

    paths should (be(Seq(Seq(graph.b, graph.ba1, graph.a)))
      or be(Seq(Seq(graph.b, graph.ba2, graph.a))))
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
        val start = sb.newState(isStartState = true)
        val a = sb.newState()
        val end = sb.newState(isFinalState = true, predicate = (n: Long) => n == d)
        start.addRelationshipExpansion(a, direction = Direction.OUTGOING)
        a.addNodeJuxtaposition(start)
        a.addNodeJuxtaposition(end)
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
      .into(d, SearchMode.Unidirectional)
      .grouped
      .withK(2)
      .toList

    // prior to the bugfix, this would have yielded Seq(1, 2, 3)
    lengths shouldBe Seq(1, 2)
  }

  test("intoTarget does not yield paths to other targets that could be valid") {
    val graph = `(a)<--(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa(singleRelPath)
      .into(graph.c)
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.bc, graph.c))
  }

  test("non-inlined prefilter applies before K limit") {
    val graph = `(a)-->(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.a)
      .withNfa(anyDirectedPath)
      .filter(p => pathLength(p) > 1)
      .withK(1)
      .paths()

    paths shouldBe Seq(Seq(graph.a, graph.ab, graph.b, graph.bc, graph.c))
  }

  test("non-inlined prefilter applies after projection") {
    val graph = `(a)-->(b)-->(c)`

    val lengths = fixture()
      .withGraph(graph.graph)
      .from(graph.a)
      .withNfa(anyDirectedPath)
      .project(pathLength)
      .filter(_ > 0)
      .toList

    lengths shouldBe Seq(1, 2)
  }

  test("bidirectional search does not yield a repeated relationship if encountered from the RHS") {
    /*
      (n1)--(n2)--(n3)--(n4)--(n5)--()
      /  \
     ()   ()
     */
    val graph = TestGraph.builder
    val n1 = graph.node()
    val n2 = graph.node()
    val n3 = graph.node()
    val n4 = graph.node()
    val n5 = graph.node()

    val r6 = graph.rel(n1, n2)
    val r7 = graph.rel(n2, n3)
    val r8 = graph.rel(n3, n4)
    val r9 = graph.rel(n4, n5)

    graph.rel(n5, graph.node())

    graph.rel(n1, graph.node())
    graph.rel(n1, graph.node())

    val paths = fixture()
      .withGraph(graph.build())
      .from(n1)
      .into(n5)
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val a = sb.newState("a")
        val b = sb.newState("b")
        val c = sb.newState("c")
        val t = sb.newState("t", isFinalState = true)

        s.addNodeJuxtaposition(a)
        a.addRelationshipExpansion(b, direction = Direction.BOTH)
        b.addRelationshipExpansion(c, direction = Direction.BOTH)
        c.addNodeJuxtaposition(a)
        c.addNodeJuxtaposition(t)
      }
      .logged()
      .paths()

    paths shouldBe Seq(Seq(n1, n1, r6, n2, r7, n3, n3, r8, n4, r9, n5, n5))
  }

  test("max depth") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    graph.rel(a, b)
    graph.rel(b, c)
    val ac = graph.rel(a, c)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(c)
      .withMaxDepth(1)
      .withNfa(anyDirectedPath)
      .paths()

    paths shouldBe Seq(Seq(a, ac, c))
  }

  /*******************************************
   * Algorithm introspection via event hooks *
   *******************************************/

  test("intoTarget initiates a bidirectional search") {
    val graph = TestGraph.builder
    val s1 = graph.node()
    val n2 = graph.node()
    val n3 = graph.node()
    val t4 = graph.node()

    graph.rel(s1, n2)
    graph.rel(s1, n3)
    graph.rel(n2, t4)

    val events = fixture()
      .withGraph(graph.build())
      .from(s1)
      .into(t4)
      .withK(1)
      .withNfa(anyDirectedPath)
      .events()

    events should (contain(Expand(TraversalDirection.Backward, 2, 1)))
  }

  test("bidirectional search stops searching if the component around the target is exhausted") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    graph.rel(a, b)
    graph.rel(a, c)

    val d = graph.node()

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(d)
      .withK(1)
      .withNfa(`(s) ((a)-->(b))* (t)`)
      .events()

    events.ofType[ExpandNode] shouldBe Seq(
      ExpandNode(a, TraversalDirection.Forward),
      ExpandNode(d, TraversalDirection.Backward)
    )
    events.ofType[ReturnPath] shouldBe Seq.empty
  }

  test("bidirectional search stops searching if the component around the source is exhausted") {
    val graph = TestGraph.builder
    val a = graph.node()

    val b = graph.node()

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(b)
      .withK(1)
      .withNfa(`(s) ((a)-->(b))* (t)`)
      .events()

    events.ofType[ExpandNode] shouldBe Seq(ExpandNode(a, TraversalDirection.Forward))
    events.ofType[ReturnPath] shouldBe Seq.empty
  }

  test("intoTarget stops searching the graph after the target is saturated") {
    val graph = `(a)-->(b)-->(c)`

    val events = fixture()
      .withGraph(graph.graph)
      .from(graph.a)
      .into(graph.b)
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
      .into(c, SearchMode.Unidirectional)
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
    val graph = `(a)<--(b)-->(c)`

    val events = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .into(graph.c)
      .withNfa(anyDirectedPath)
      .events()

    events should (contain(AddTarget(graph.c)) and not contain AddTarget(graph.a))
  }

  test("algorithm should exit once we hit max depth") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    graph.rel(a, b)
    graph.rel(b, c)
    graph.rel(a, c)

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(c)
      .withMaxDepth(1)
      .withNfa(anyDirectedPath)
      .events()

    events should not contain NextLevel(2)
  }

  /*******************
   * Memory tracking *
   *******************/

  test("memory tracking") {
    val graph = `(a)-->(b)`

    val mt = new LocalMemoryTracker()

    // ignore the memory tracker for path tracer for the purposes of this test
    def createPathTracer(_mt: MemoryTracker, hooks: PPBFSHooks): PathTracer =
      new PathTracer(EmptyMemoryTracker.INSTANCE, hooks)

    val iter = fixture()
      .withGraph(graph.graph)
      .from(graph.a)
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

  /****************
   * Interruption *
   ****************/

  test("can be interrupted by AssertOpen check") {
    val graph = `(a)-->(b)`

    val iter = fixture()
      .withGraph(graph.graph)
      .from(graph.a)
      .withNfa(anyDirectedPath)
      .onAssertOpen {
        throw new Exception("boom")
      }
      .build()

    the[Exception] thrownBy iter.asScala.toList should have message "boom"
  }

  /***********************************************************************
  * Generated graphs/fixtures, testing for equivalence with naive search *
  ***********************************************************************/

  for {
    nfa <- Seq(
      `(s) ((a)-->(b))* (t)`,
      `(s) ((a)-->(b))+ (t)`,
      `(s) ((a)--(b))+ (t)`,
      `(s) ((a)--(b)--(c))* (t)`
    )
    graph <- testGraphs
    into <- Seq(true, false)
    grouped <- Seq(true, false)
    k <- Seq(Int.MaxValue, 1, 2)
  } {
    test(
      s"running the algorithm gives the same results as naive search. into=$into grouped=$grouped k=$k nfa=$nfa graph=${graph.render}"
    ) {
      var f = fixture()
        .withGraph(graph.graph)
        .from(graph.source)
        .withNfa(nfa)

      if (into) {
        f = f.into(graph.target)
      }

      if (grouped) {
        f = f.grouped
      }

      if (k != Int.MaxValue) {
        f = f.withK(k)
      }

      f.assertExpected()
    }
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
        s.addRelationshipExpansion(s, direction = Direction.OUTGOING)
      }
      .from(a)
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
        val s = sb.newState("s", isStartState = true)
        s.addRelationshipExpansion(s)
        val e = sb.newState("e", isFinalState = true)
        s.addNodeJuxtaposition(e)
      }
      .from(a)
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
        val s = sb.newState("s", isStartState = true)
        val e = sb.newState("e", isFinalState = true)

        s.addRelationshipExpansion(e)
        e.addNodeJuxtaposition(s)
      }
      .from(a)
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
        val s = sb.newState("s", isStartState = true)
        val a = sb.newState("a")
        val e = sb.newState("e", isFinalState = true)

        s.addRelationshipExpansion(a)
        a.addNodeJuxtaposition(s)
        a.addNodeJuxtaposition(e)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, ab, b)
    )
  }

  /** Recursively compares groups of paths until we reach an expected group that is smaller than the remaining k */
  private def compareGroups(
    resultGroups: List[Set[TracedPath]],
    expectedGroups: List[Set[TracedPath]],
    k: Int
  ): Unit = {
    (resultGroups, expectedGroups) match {
      case (res :: resTail, exp :: expTail) if exp.size <= k =>
        res shouldBe exp
        compareGroups(resTail, expTail, k - exp.size)

      case (res :: _, exp :: _) =>
        res.size shouldBe k
        exp should contain allElementsOf res

      case (res, Nil) =>
        res shouldBe empty

      case (Nil, _) =>
        k shouldBe 0
    }
  }

  case class FixtureBuilder[A](
    graph: TestGraph,
    nfa: PGStateBuilder,
    source: Long,
    intoTarget: Long,
    searchMode: SearchMode,
    isGroup: Boolean,
    maxDepth: Int,
    k: Int,
    projection: TracedPath => A,
    predicate: A => Boolean,
    mt: MemoryTracker,
    hooks: PPBFSHooks,
    assertOpen: AssertOpen
  ) {

    def withGraph(graph: TestGraph): FixtureBuilder[A] = copy(graph = graph)

    def withNfa(f: PGStateBuilder => Unit): FixtureBuilder[A] = copy(nfa = {
      val builder = new PGStateBuilder
      f(builder)
      builder
    })
    def from(source: Long): FixtureBuilder[A] = copy(source = source)

    def into(intoTarget: Long, searchMode: SearchMode): FixtureBuilder[A] =
      copy(intoTarget = intoTarget, searchMode = searchMode)

    def into(intoTarget: Long): FixtureBuilder[A] =
      into(intoTarget, if (intoTarget == -1L) SearchMode.Unidirectional else SearchMode.Bidirectional)

    def withMode(searchMode: SearchMode): FixtureBuilder[A] = copy(searchMode = searchMode)
    def grouped: FixtureBuilder[A] = copy(isGroup = true)
    def withMaxDepth(maxDepth: Int): FixtureBuilder[A] = copy(maxDepth = maxDepth)
    def withK(k: Int): FixtureBuilder[A] = copy(k = k)
    def withMemoryTracker(mt: MemoryTracker): FixtureBuilder[A] = copy(mt = mt)
    def onAssertOpen(assertOpen: => Unit): FixtureBuilder[A] = copy(assertOpen = () => assertOpen)

    /** NB: wipes any configured filter, since the iterated item type will change */
    def project[B](projection: TracedPath => B): FixtureBuilder[B] =
      FixtureBuilder[B](
        graph,
        nfa,
        source,
        intoTarget,
        searchMode,
        isGroup,
        maxDepth,
        k,
        projection,
        _ => true,
        mt,
        hooks,
        assertOpen
      )
    def filter(predicate: A => Boolean): FixtureBuilder[A] = copy(predicate = predicate)

    def build(createPathTracer: (MemoryTracker, PPBFSHooks) => PathTracer = new PathTracer(_, _)) =
      new PGPathPropagatingBFS[A](
        source,
        nfa.getStart.state,
        intoTarget,
        nfa.getFinal.state,
        searchMode,
        new MockGraphCursor(graph),
        createPathTracer(mt, hooks),
        projection(_),
        predicate(_),
        isGroup,
        maxDepth,
        k,
        nfa.stateCount,
        mt,
        hooks,
        assertOpen
      )

    def toList: Seq[A] = build().asScala.toList

    def logged(level: LoggingPPBFSHooks = LoggingPPBFSHooks.debug): FixtureBuilder[A] = copy(hooks = level)

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

    /** Runs a naive/slow DFS equivalent of the algorithm and compares results with the real implementation */
    def assertExpected()(implicit ev: A =:= TracedPath): Unit = {
      val result = this.toList.map(ev)
      val expected = allPaths()

      if (k == Int.MaxValue) {
        // with no K value set, we can just compare all the paths without worrying about selectivity
        result should contain theSameElementsAs expected
      } else {
        // if we have a limit then we need to ensure that the paths beneath that limit have been returned
        val orderedResults = OrderedResults.fromSeq(result)
        val orderedExpected = OrderedResults.fromSeq(expected)
        if (isGroup) {
          // this is the simpler case: we can apply the K limit to each target list since they are already grouped
          orderedResults shouldBe orderedExpected.takeGroups(k)
        } else {
          // this is the most complex case since for each target we have to account for nondeterminism in chosen paths
          orderedResults.targets shouldBe orderedExpected.targets

          for ((target, resultPaths) <- orderedResults.byTargetThenLength) {
            val expectedPaths = orderedExpected.paths(target)

            compareGroups(resultPaths, expectedPaths, k)
          }
        }
      }
    }

    /** Runs a recursive exhaustive depth first search to provide a comparison.
     * Note that this does not truncate the result set by K, since that would be nondeterministic for non-grouped cases */
    def allPaths()(implicit ev: A =:= TracedPath): Seq[TracedPath] = {
      def recurse(stack: List[PathEntity], node: Long, state: State): Seq[TracedPath] = {
        val nodeJuxtapositions = state.getNodeJuxtapositions
          .filter(nj => nj.targetState().test(node))
          .flatMap(nj => recurse(PathEntity.fromNode(nj.targetState(), node) :: stack, node, nj.targetState()))

        val relExpansions = for {
          re <- state.getRelationshipExpansions
          (rel, dir) <- graph.rels(node)
          nextNode = dir match {
            case RelationshipDirection.OUTGOING => rel.target
            case RelationshipDirection.INCOMING => rel.source
            case RelationshipDirection.LOOP     => node
            case _                              => fail("inexhaustive match")
          }
          if dir.matches(re.direction) &&
            re.testRelationship(rel) &&
            re.targetState().test(nextNode) &&
            !stack.exists(e => e.id() == rel.id)

          newStack = PathEntity.fromNode(re.targetState(), nextNode) ::
            PathEntity.fromRel(re, rel.id) ::
            stack

          paths <- recurse(newStack, nextNode, re.targetState())
        } yield paths

        val wholePath = Option.when(state.isFinalState && (intoTarget == -1L || intoTarget == node))(new TracedPath(
          stack.reverse.toArray
        ))

        nodeJuxtapositions ++ relExpansions ++ wholePath
      }

      recurse(List(PathEntity.fromNode(nfa.getStart.state, source)), source, nfa.getStart.state)
        .filter(path => this.predicate(ev.flip(path)))
    }
  }

  private def fixture() = FixtureBuilder[TracedPath](
    graph = TestGraph.empty,
    nfa = new PGStateBuilder,
    source = -1L,
    intoTarget = -1L,
    SearchMode.Unidirectional,
    isGroup = false,
    maxDepth = -1,
    k = Int.MaxValue,
    projection = identity,
    predicate = _ => true,
    mt = EmptyMemoryTracker.INSTANCE,
    hooks = PPBFSHooks.NULL,
    assertOpen = () => ()
  )
}

object PGPathPropagatingBFSTest {

  private def anyDirectedPath(sb: PGStateBuilder): Unit = {
    val s = sb.newState(isStartState = true, isFinalState = true)
    s.addRelationshipExpansion(s, direction = Direction.OUTGOING)
  }

  private def singleRelPath(sb: PGStateBuilder): Unit = {
    sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
  }

  private def pathLength(path: TracedPath) = path.entities().count(_.entityType() == EntityType.RELATIONSHIP)
  private def target(path: TracedPath) = path.entities().last.id()

  /** Divides up paths by their target, then chunks them into groups of ascending path length */
  case class OrderedResults(byTargetThenLength: Map[Long, List[Set[TracedPath]]]) {
    def targets: Set[Long] = byTargetThenLength.keySet
    def paths(target: Long): List[Set[TracedPath]] = byTargetThenLength(target)

    /** Applies the K limit to each target group list */
    def takeGroups(k: Int): OrderedResults = copy(byTargetThenLength =
      byTargetThenLength
        .view
        .mapValues(_.take(k))
        .toMap
    )
  }

  object OrderedResults {

    def fromSeq(seq: Seq[TracedPath]): OrderedResults = OrderedResults(
      seq
        .groupBy(target)
        .view
        .mapValues(_.groupBy(pathLength).toList.sortBy(_._1).map(_._2.toSet))
        .toMap
    )

  }

  // noinspection TypeAnnotation
  private object `(a)-->(b)` {
    private val g = TestGraph.builder
    val a = g.node()
    val b = g.node()
    val ab = g.rel(a, b)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)<--(b)-->(c)` {
    private val g = TestGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val ba = g.rel(b, a)
    val bc = g.rel(b, c)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)-->(b)-->(c)` {
    private val g = TestGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val ab = g.rel(a, b)
    val bc = g.rel(b, c)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)-->(b)<--(c)` {
    private val g = TestGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val ab = g.rel(a, b)
    val cb = g.rel(c, b)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)<--(b)-->(a)` {
    private val g = TestGraph.builder
    val a = g.node()
    val b = g.node()
    val ba1 = g.rel(b, a)
    val ba2 = g.rel(b, a)
    val graph = g.build()
  }

  private class Nfa(name: String, construct: PGStateBuilder => Unit) extends Function[PGStateBuilder, Unit] {
    override def toString: String = name
    def apply(v1: PGStateBuilder): Unit = construct(v1)
  }

  private def nfa(name: String)(construct: PGStateBuilder => Unit): Nfa = new Nfa(name, construct)

  private val `(s) ((a)-->(b))* (t)` : Nfa = nfa("(s) ((a)-->(b))* (t)") { sb =>
    val s = sb.newState("s", isStartState = true)
    val a = sb.newState("a")
    val b = sb.newState("b")
    val t = sb.newState("t", isFinalState = true)

    s.addNodeJuxtaposition(a)
    a.addRelationshipExpansion(b, direction = Direction.OUTGOING)
    b.addNodeJuxtaposition(a)
    b.addNodeJuxtaposition(t)
    s.addNodeJuxtaposition(t)
  }

  private val `(s) ((a)-->(b))+ (t)` : Nfa = nfa("(s) ((a)-->(b))+ (t)") { sb =>
    val s = sb.newState("s", isStartState = true)
    val a = sb.newState("a")
    val b = sb.newState("b")
    val t = sb.newState("t", isFinalState = true)

    s.addNodeJuxtaposition(a)
    a.addRelationshipExpansion(b, direction = Direction.OUTGOING)
    b.addNodeJuxtaposition(a)
    b.addNodeJuxtaposition(t)
  }

  private val `(s) ((a)--(b))+ (t)` : Nfa = nfa("(s) ((a)--(b))+ (t)") { sb =>
    val s = sb.newState("s", isStartState = true)
    val a = sb.newState("a")
    val b = sb.newState("b")
    val t = sb.newState("t", isFinalState = true)

    s.addNodeJuxtaposition(a)
    a.addRelationshipExpansion(b, direction = Direction.BOTH)
    b.addNodeJuxtaposition(a)
    b.addNodeJuxtaposition(t)
  }

  private val `(s) ((a)--(b)--(c))* (t)` : Nfa = nfa("(s) ((a)--(b)--(c))* (t)") { sb =>
    val s = sb.newState("s", isStartState = true)
    val a = sb.newState("a")
    val b = sb.newState("b")
    val c = sb.newState("c")
    val t = sb.newState("t", isFinalState = true)

    s.addNodeJuxtaposition(a)
    a.addRelationshipExpansion(b, direction = Direction.BOTH)
    b.addRelationshipExpansion(c, direction = Direction.BOTH)
    c.addNodeJuxtaposition(a)
    c.addNodeJuxtaposition(t)
    s.addNodeJuxtaposition(t)
  }

  private case class NamedGraph(render: String, graph: TestGraph, source: Long, target: Long)

  /** a generated series of graphs consisting of a variable length chain of relationships from (start) to (end),
   * with another variable length chain connecting two nodes from the original */
  private val testGraphs: Seq[NamedGraph] = {
    for {
      mainLength <- 1 to 4
      g2 = TestGraph.empty.line(mainLength)
      start = g2.nodes.min
      end = g2.nodes.max

      n1 <- g2.nodes
      n2 <- g2.nodes
      secondaryLength <- 1 to 3
      g3 = g2.chainRel(n1, n2, secondaryLength)
    } yield NamedGraph(
      s"(n$start)-$mainLength->(n$end), (n$n1)-$secondaryLength->(n$n2)",
      g3,
      start,
      end
    )
  }

}

case class TestGraph(nodes: Set[Long], rels: Set[TestGraph.Rel]) {
  def lastId: Long = nodes.size + rels.size
  def nextId: Long = lastId + 1

  def withNode(): TestGraph = copy(nodes = nodes + nextId)

  def withRel(source: Long, target: Long, relType: Int): TestGraph = {
    assert(nodes.contains(source) && nodes.contains(target))
    copy(rels = rels + Rel(nextId, source, target, relType))
  }

  def rels(node: Long): Iterator[(Rel, RelationshipDirection)] =
    rels.iterator.collect {
      case rel if node == rel.source && node == rel.target =>
        rel -> RelationshipDirection.LOOP
      case rel if node == rel.source =>
        rel -> RelationshipDirection.OUTGOING
      case rel if node == rel.target =>
        rel -> RelationshipDirection.INCOMING
    }

  def builder = new TestGraph.Builder(this)

  def line(length: Int): TestGraph =
    withBuilder(_.line(length))

  def chainRel(source: Long, target: Long, length: Int): TestGraph =
    withBuilder(_.chainRel(source, target, length))

  private def withBuilder(f: TestGraph.Builder => Unit): TestGraph = {
    val b = builder
    f(b)
    b.build()
  }
}

object TestGraph {

  case class Rel(id: Long, source: Long, target: Long, relType: Int) extends RelationshipDataReader {

    def opposite(x: Long): Long =
      x match {
        case `target` => source
        case `source` => target
        case _        => throw new IllegalArgumentException()
      }

    override def toString: String = s"($source)-[$id:$relType]->($target)"

    def relationshipReference(): Long = id
    def `type`(): Int = relType
    def sourceNodeReference(): Long = source
    def targetNodeReference(): Long = target
    def source(cursor: NodeCursor): Unit = ???
    def target(cursor: NodeCursor): Unit = ???
  }

  val empty: TestGraph = TestGraph(Set.empty, Set.empty)
  def builder = empty.builder

  class Builder(private var graph: TestGraph) {

    def node(): Long = {
      val nextId = graph.nextId
      graph = graph.withNode()
      nextId
    }

    def rel(source: Long, target: Long): Long = rel(source, target, 1)

    def rel(source: Long, target: Long, relType: Int): Long = {
      val nextId = graph.nextId
      graph = graph.withRel(source, target, relType)
      nextId
    }

    def line(length: Int): Seq[Long] = {
      val nodes = (0 to length).map(_ => node())
      nodes.zip(nodes.drop(1)).foreach { case (a, b) => rel(a, b) }
      nodes
    }

    def chainRel(source: Long, target: Long, length: Int): Unit = {
      var current = source
      for (_ <- 1 until length) {
        val next = node()
        rel(current, next)
        current = next
      }
      rel(current, target)
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
