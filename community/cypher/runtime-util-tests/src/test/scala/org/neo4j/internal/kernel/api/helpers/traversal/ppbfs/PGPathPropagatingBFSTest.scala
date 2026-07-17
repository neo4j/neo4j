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

import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.runtime.RuntimeUtilTestSuite
import org.neo4j.cypher.internal.util.test_helpers.InMemoryGraph
import org.neo4j.function.Predicates
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NfaDsl.Implicits._
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest._
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTestBase.Nfa
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTestBase.nfa
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.AddTarget
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.Expand
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.ExpandNode
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.NextLevel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.ReturnPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipPredicate
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker

import java.util.function.LongPredicate

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.language.postfixOps

class PGPathPropagatingBFSTest extends RuntimeUtilTestSuite with PGPathPropagatingBFSTestBase {

  /*************************************
   * simple tests on the nfa traversal *
   *************************************/

  test("single node is yielded when state is both start and final state") {
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = `(n1)-->(n2)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n1, graph.r1_2, graph.n2))
  }

  test(
    "relationship expansions can traverse a relationship in the inverse direction"
  ) {
    val graph = `(n1)-->(n2)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n2)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n2, graph.r1_2, graph.n1))
  }

  test("relationship expansions can traverse a loop") {
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = `(n1)-->(n2)-->(n3)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n2)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, direction = Direction.INCOMING)
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n2, graph.r1_2, graph.n1))
  }

  test("relationship expansions may be filtered on OUTGOING direction") {
    val graph = `(n1)-->(n2)-->(n3)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n2)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, direction = Direction.OUTGOING)
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n2, graph.r2_3, graph.n3))
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
    val graph = `(n1)-->(n2)-->(n3)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa { sb =>
        val s = sb.newState()
        sb.newState(isStartState = true).addRelationshipExpansion(s)
        s.addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n1, graph.r1_2, graph.n2, graph.r2_3, graph.n3))
  }

  /********************************
   * More complex NFAs and graphs *
   ********************************/

  test("paths of different lengths are yielded in order of length") {
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = `(n1)-->(n2)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(`(s) ((a)--(b))+ (t)`)
      .paths()

    paths shouldBe Seq(
      Seq(graph.n1, graph.n1, graph.r1_2, graph.n2, graph.n2)
    )
  }

  test("loops are traversed in any order") {
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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

  // a test configuration that was found in the generated StatefulShortest tests
  // replicated here because these tests are faster and better encapsulated.
  test("cobweb of doom") {
    val OUT = 1
    val SIDEWAYS = 2
    val RANDOM = 3

    val (graph, n0, n4, n7) = {
      val g = InMemoryGraph.builder

      val n0 = g.node()
      val n1 = g.node()
      val n2 = g.node()
      val n3 = g.node()
      val n4 = g.node()
      val n5 = g.node()
      val n6 = g.node()
      val n7 = g.node()
      val n8 = g.node()

      g.rel(n0, n1, OUT)
      g.rel(n1, n2, OUT)
      g.rel(n2, n3, OUT)
      g.rel(n3, n4, OUT)

      g.rel(n0, n5, OUT)
      g.rel(n5, n6, OUT)
      g.rel(n6, n7, OUT)

      g.rel(n0, n8, OUT)

      g.rel(n8, n1, SIDEWAYS)
      g.rel(n1, n5, SIDEWAYS)
      g.rel(n5, n8, SIDEWAYS)

      g.rel(n2, n6, SIDEWAYS)

      g.rel(n3, n7, SIDEWAYS)

      g.rel(n4, n5, RANDOM)

      (g.build(), n0, n4, n7)
    }

    val n: Nfa = nfa("(s: S) ((a)-[:OUT]->(b)-[:SIDEWAYS|RANDOM]-(c)-[:OUT|RANDOM]-(d))+ (t: T)") { sb =>
      val s = sb.newState("s", isStartState = true, predicate = n => n == n0)
      val a = sb.newState("a")
      val b = sb.newState("b")
      val c = sb.newState("c")
      val d = sb.newState("d")
      val t = sb.newState("t", isFinalState = true, predicate = n => n == n4 || n == n7)

      s.addNodeJuxtaposition(a)
      a.addRelationshipExpansion(b, types = Array(OUT), direction = Direction.OUTGOING)
      b.addRelationshipExpansion(c, types = Array(SIDEWAYS, RANDOM), direction = Direction.BOTH)
      c.addRelationshipExpansion(d, types = Array(OUT, RANDOM), direction = Direction.BOTH)
      d.addNodeJuxtaposition(a)
      d.addNodeJuxtaposition(t)
    }

    fixture()
      .withGraph(graph)
      .withNfa(n)
      .withK(2)
      .grouped
      .from(n0)
      .assertExpected()
  }

  // a test that failed when trying to fix the above broken test
  test("ruthless cobweb") {
    val graph = InMemoryGraph.fromTemplate("""
         .--->(0)--->(1)--->(2)
         |                   |
         |                  [:R]
         |    .---[:R]--.    |
         |    |         v    v
        (3)->(4)------>(5)->(6)->(7)
         |    ^
         |    |
         |   [:R]
         |    |
         '-->(8)
      """)
    val NOT_R = InMemoryGraph.DEFAULT_REL
    val R = graph relTypes "R"
    val s = graph node "3"

    val isTarget: LongPredicate = {
      val t1 = graph node "7"
      val t2 = graph node "8"
      n => n == t1 || n == t2
    }

    import NfaDsl.Implicits._
    val n: Nfa = nfa("s" |>
      ("a" - NOT_R - "b" +) |>
      ("c" - R - "d") |>
      ("e" - NOT_R - "f" +) |>
      ("t" where isTarget))

    fixture()
      .withGraph(graph)
      .from(s)
      .withNfa(n)
      .withK(3)
      .grouped
      .assertExpected()
  }

  // a test that is simplified from the above, but still showed the same error, thus easier to debug
  test("ruthless cobweb mostly simplified") {
    val graph = InMemoryGraph.fromTemplate("""

      (0:T)-->(1)-->(2)-->(3:T)
       ^       ^
       '--(4)--'
      """)

    val s = graph node "1"

    import NfaDsl.Implicits._
    val n: Nfa = nfa("s" |> ("a" -- "b" +) |> ("t" where graph.hasLabel("T")))

    fixture()
      .withGraph(graph)
      .from(s)
      .withNfa(n)
      .withK(4)
      .grouped
      .assertExpected()
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
      .project(TracedPath.fromSignpostStack(_).length)
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
    val graph = `(n1)-->(n2)-->(n3)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(anyDirectedPath)
      .filter(p => p.length > 1)
      .withK(1)
      .paths()

    paths shouldBe Seq(Seq(graph.n1, graph.r1_2, graph.n2, graph.r2_3, graph.n3))
  }

  test("non-inlined prefilter applies after projection") {
    val graph = `(n1)-->(n2)-->(n3)`

    val lengths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(anyDirectedPath)
      .project(TracedPath.fromSignpostStack(_).length)
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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

    events should (contain(Expand(TraversalDirection.BACKWARD, 2, 1)))
  }

  test("bidirectional search stops searching if the component around the target is exhausted") {
    val graph = InMemoryGraph.builder
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
      ExpandNode(a, TraversalDirection.FORWARD),
      ExpandNode(d, TraversalDirection.BACKWARD)
    )
    events.ofType[ReturnPath] shouldBe Seq.empty
  }

  test("bidirectional search stops searching if the component around the source is exhausted") {
    val graph = InMemoryGraph.builder
    val a = graph.node()

    val b = graph.node()

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(b)
      .withK(1)
      .withNfa(`(s) ((a)-->(b))* (t)`)
      .events()

    events.ofType[ExpandNode] shouldBe Seq(ExpandNode(a, TraversalDirection.FORWARD))
    events.ofType[ReturnPath] shouldBe Seq.empty
  }

  test("intoTarget stops searching the graph after the target is saturated") {
    val graph = `(n1)-->(n2)-->(n3)`

    val events = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .into(graph.n2)
      .withK(1)
      .withNfa(anyDirectedPath)
      .events()

    events should (contain(NextLevel(1)) and not contain NextLevel(2))
  }

  test("propagation is performed when a previously-seen node is encountered") {
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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

  /****************************************
   * Target node pruning regression cases *
   ****************************************/

  test("target node purging challenger") {

    // This test highlights a problem that any target signpost purging solution we've tried has failed to solve.
    // Namely that after we've purged target a given signpost, which we later re-register towards a new target,
    // we're not guaranteed to register it at its minimum distance first.

    // We first explain the ingredients to the test, after which we explain the cause of failure
    //
    // INGREDIENTS:
    //
    // * A SHORTEST 2 path selector
    //
    // * We need a target signpost `problem_sp` which lies on two paths to a target t1, p1 of length l1 and p2 of length l2
    //   where, problem_sp is at index i1 in p1 and i2 in p2, where:
    //   - l1 < l2
    //   - l2 - i2 < l1 - i1    (I.e even if p1 is shorter, the subpath that remains after tsp is shorter in p2.)
    //   - the path p0 = p1.subpath(0, i1).concat(p2.subpath(i2, l2)) shouldn't be a trail in the data graph
    //
    // * We then need a target t2 further away, that has t1 on it's way back from source like
    //    (s) -- ... -->(t) --- ... --->(t2)
    //
    // * And we need a longer path from (s) to tsp.prevNode which is completely disconnected from the rest
    //   of the graph, and longer than the shortest path we trace from (s) to (t2)
    //
    // Model for how the p0,p1,p2 paths can look:
    //
    //  p0 = (s)-...->()-[dup]->()-...->()-[problem_sp]->()-...->()-[dup]->()-...->(t)
    //
    //  p1 = (s)-...->()-[dup]->()-...->()-[problem_sp]->()-...->(t)
    //
    //  p2 = (s)-...->()-[tsp]->()-...->()-[problem_sp]->()-...->(t)
    //
    //
    // This model fits the following pattern,
    //
    //  (s) (()-[:DUP|R1|R3]->())+ ()-[:PROBLEM]->() (()-[:DUP|R2]->())+ (()-[:REST]->())* (:T)
    //
    // on the following graphs,
    //
    //
    //                    ┌───────────PROBLEM────────────┐
    //                    │            ┌──┐              │
    //                    │    ┌──R1───▶n2│───R1───┐     │
    //                    │ ┌──┴─┐     └──┘       ┌▼───┐ │
    //                    │ │    │                │    ├─┘
    //                    └─▶n0:S│──────DUP──────▶│ n1 │      ┌─────┐      ┌───┐      ┌───┐      ┌─────┐
    //                      │    │                │    ├─REST─▶n13:T├─REST─▶n14├─REST─▶n15├─REST─▶n16:T│
    //                      └┬─┬─┘ ┌──┐    ┌──┐   └▲─▲─┘      └─────┘      └───┘      └───┘      └─────┘
    //                       │ └R2─▶n3├─R2─▶n4├─R2─┘ │
    //    ┌─────────R3───────┘     └──┘    └──┘      └────────R3───────┐
    //    │┌──┐    ┌──┐    ┌──┐    ┌──┐    ┌──┐    ┌───┐    ┌───┐    ┌─┴─┐
    //    └▶n5├─R3─▶n6├─R3─▶n7├─R3─▶n8├─R3─▶n9├─R3─▶n10├─R3─▶n11├─R3─▶n12│
    //     └──┘    └──┘    └──┘    └──┘    └──┘    └───┘    └───┘    └───┘

    // where the paths p0,p1,p2 are given by
    //
    // p0 = (s)-[:DUP]->(n1)-[sp_problem:PROBLEM]->(s)-[:DUP]->()-[:REST]->(t1)
    //
    // p1 = (s)-[:DUP]->(n1)-[sp_problem:PROBLEM]->(s)-[:R1 * 2]->()-[:REST]->(t1)
    //
    // p2 = (s)-[:R2 *3]->(n1)-[sp_problem:PROBLEM]->(s)-[:DUP]->()-[:REST]->(t1)
    //
    //
    // CAUSE OF FAILURE:
    //
    // This is what will happen (in an algorithm with faulty target signpost purging):
    //
    // * We will trace p0 first, but since it isn't a trail in the data graph, we'll prune (among other source signposts)
    //   the source signpost corresponding to sp_problem at lengthFromSource == 3. We will however add
    //   minDistToTarget==2 to the target signpost corresponding to sp_problem
    //
    // * We then trace p1 and add lengthFromSource == 4 to the source signpost corresponding to sp_problem.
    //   We also decrement the remainingTargetCount to 1
    //
    // * We then trace p2 and add lengthFromSource == 5 to the source signpost corresponding to sp_problem.
    //   We also decrement the remainingTargetCount to 0, and set minDistToTarget==NO_SUCH_DISTANCE in the
    //   target signpost corresponding to sp_problem during target signpost purging
    //
    // * Then when the BFS reaches t2, it will try to trace the path from (s) to (t2) of length 7, however,
    //   when the tracer reaches (t1) it will stop since (t1) won't have any source signpost with
    //   lengthFromSource==4, since this was pruned when we traced p0. This is still correct!
    //   When it tries to trace this path, it will register target signposts the whole way from t1 to t2,
    //   and t1 will be registered to propagate the length data corresponding to p1,
    //
    // * We will then trace the path
    //
    //    (s)-[:DUP]->(n1)-[sp_problem:PROBLEM]->(s)-[:R1 * 2]->()-[:REST]->(t1)-[:REST * 3]->(t2),
    //
    //   and when we do this we will re-set the minDistToTarget in the target signpost corresponding to sp_problem
    //   to minDistToTarget=7. THIS IS INCORRECT! Indeed, sp_problem can follow an even shorter path to t2, namely,
    //
    //    (n1)-[sp_problem:PROBLEM]->(s)-[:DUP]->()-[:REST]->(t1)-[:REST * 3]->(t2)
    //
    //   with a distance to target of 6.
    //
    // * This makes it so that the longer subpath through all the :R3 rels are propagated up a non-shortest
    //   path to a target it's first propagation, and won't be traced with the shorter path to the target which we
    //   missed.

    val graph = InMemoryGraph.fromTemplate("""
                       .----------[:PROBLEM]----------.
                       | .----[:R1]--->()---[:R1]---. |
                       v |                          v |
                     (START)---------[:DUP]-------->( )-[:REST]->(:T)-[:REST]->()-[:REST]->()-[:REST]->(:T)
                       | |                          ^ ^
      .-----[:R3]------' '[:R2]->()-[:R2]->()-[:R2]-' '------[:R3]---------.
      v                                                                    |
     ()-[:R3]->()-[:R3]->()-[:R3]->()-[:R3]->()-[:R3]->()-[:R3]->()-[:R3]->()
        """)

    val R1 = graph relTypes "R1"
    val R2 = graph relTypes "R2"
    val R3 = graph relTypes "R3"
    val DUP = graph relTypes "DUP"
    val PROBLEM = graph relTypes "PROBLEM"
    val REST = graph relTypes "REST"

    val nfa = {
      import NfaDsl.Implicits._

      import scala.language.postfixOps
      // format: off
      "s"
        .|> ("a"-(DUP:|R1:|R3)->"b"+)
        .|> ("c"-PROBLEM->"d")
        .|> ("e"-(DUP:|R2)->"f"+)
        .|> (("g"-REST->"h").*)
        .|> ("t" where graph.hasLabel("T"))
      // format: on
    }

    val lengths = fixture()
      .withGraph(graph)
      .withNfa(nfa.build)
      .from(graph node "START")
      .withK(2)
      .project(TracedPath.fromSignpostStack(_).length)
      .filter(len => len != 9 && len != 10)
      .toList

    lengths shouldBe Seq(5, 6, 8, 15)
  }

  // Regression test: trail-mode pruning must not destroy BFS-discovered source lengths.
  // In trail mode, pruning can destroy BFS-discovered source lengths at signposts, which
  // prevents valid trails from being found at greater depths. Without the fix (protecting
  // BFS source lengths from pruning in trail mode), the signpost chain to the target at n6
  // becomes permanently broken and the target is never found.
  //
  // Graph structure (13 nodes, 14 rels):
  //   n0(S) --OUT--> n4 --OUT--> n5 --OUT--> n6(T)    (spoke A: source to target)
  //   n0(S) --OUT--> n7 --OUT--> n8                    (spoke B)
  //   n9 --OUT--> n10 --OUT--> n11                     (spoke C, disconnected from source)
  //   n1 --OUT--> n2 --OUT--> n3(T)                    (spoke D, disconnected from source)
  //   Cross links (SIDEWAYS): n7->n9, n1->n5, n5->n8, n11->n2, n2->n4, n1->n7
  //
  // NFA: (s:S) ((a) -[R|S]-> (b) -[S]- (c) -[R|S]-> (d))+ (t:T)
  test("trail pruning preserves BFS source lengths in cobweb graph") {
    val R = 2 // "regular" directed rel type
    val S = 3 // "sideways" undirected rel type

    val S_LABEL = 1L
    val T_LABEL = 2L

    val g = InMemoryGraph.builder

    // Source
    val n0 = g.node(S_LABEL)

    // Chain D: n1 -> n2 -> n3(T) (not directly connected to source)
    val n1 = g.node()
    val n2 = g.node()
    val n3 = g.node(T_LABEL)
    g.rel(n1, n2, R)
    g.rel(n2, n3, R)

    // Spoke A: n0 -> n4 -> n5 -> n6(T)
    val n4 = g.node()
    val n5 = g.node()
    val n6 = g.node(T_LABEL)
    g.rel(n0, n4, R)
    g.rel(n4, n5, R)
    g.rel(n5, n6, R)

    // Spoke B: n0 -> n7 -> n8
    val n7 = g.node()
    val n8 = g.node()
    g.rel(n0, n7, R)
    g.rel(n7, n8, R)

    // Chain C: n9 -> n10 -> n11 (not directly connected to source)
    val n9 = g.node()
    val n10 = g.node()
    val n11 = g.node()
    g.rel(n9, n10, R)
    g.rel(n10, n11, R)

    // Cross links (sideways)
    g.rel(n7, n9, S)
    g.rel(n1, n5, S)
    g.rel(n5, n8, S)
    g.rel(n11, n2, S)
    g.rel(n2, n4, S)
    g.rel(n1, n7, S)

    val graph = g.build()

    val isSource: LongPredicate = n => n == n0
    val isTarget: LongPredicate = n => n == n3 || n == n6

    import NfaDsl.Implicits._
    // NFA: (s:S) ((a) -[R|S]-> (b) -[S]- (c) -[R|S]-> (d))+ (t:T)
    val n: Nfa = nfa(
      ("s" where isSource) |>
        ("a" - (R :| S) -> "b" - S - "c" - (R :| S) -> "d" +) |>
        ("t" where isTarget)
    )

    fixture()
      .withGraph(graph)
      .from(n0)
      .withNfa(n)
      .withK(1)
      .assertExpected()
  }

  // Same graph and NFA as above but in walk mode. Verifies that the BFS source length
  // protection does not interfere with walk mode (where pruning should remain unrestricted).
  test("walk mode on cobweb graph") {
    val R = 2
    val S = 3

    val S_LABEL = 1L
    val T_LABEL = 2L

    val g = InMemoryGraph.builder

    val n0 = g.node(S_LABEL)

    val n1 = g.node()
    val n2 = g.node()
    val n3 = g.node(T_LABEL)
    g.rel(n1, n2, R)
    g.rel(n2, n3, R)

    val n4 = g.node()
    val n5 = g.node()
    val n6 = g.node(T_LABEL)
    g.rel(n0, n4, R)
    g.rel(n4, n5, R)
    g.rel(n5, n6, R)

    val n7 = g.node()
    val n8 = g.node()
    g.rel(n0, n7, R)
    g.rel(n7, n8, R)

    val n9 = g.node()
    val n10 = g.node()
    val n11 = g.node()
    g.rel(n9, n10, R)
    g.rel(n10, n11, R)

    g.rel(n7, n9, S)
    g.rel(n1, n5, S)
    g.rel(n5, n8, S)
    g.rel(n11, n2, S)
    g.rel(n2, n4, S)
    g.rel(n1, n7, S)

    val graph = g.build()

    val isSource: LongPredicate = n => n == n0
    val isTarget: LongPredicate = n => n == n3 || n == n6

    import NfaDsl.Implicits._
    val n: Nfa = nfa(
      ("s" where isSource) |>
        ("a" - (R :| S) -> "b" - S - "c" - (R :| S) -> "d" +) |>
        ("t" where isTarget)
    )

    fixture()
      .withGraph(graph)
      .from(n0)
      .withNfa(n)
      .withK(1)
      .withPathMode(TraversalPathMode.Walk)
      .withMaxDepth(15)
      .assertExpected()
  }

  /**
  Walk mode
   */
  test("support walk into") {
    val graph = `(n1)-->(n2)`
    val nfa = `(s) ((a)--(b))+ (t)`

    val paths: Seq[Seq[Long]] = fixture()
      .withGraph(graph.graph)
      .withNfa(nfa)
      .withK(5)
      .from(graph.n1)
      .into(graph.n2)
      .withPathMode(TraversalPathMode.Walk)
      .withMode(SearchMode.Unidirectional)
      .paths()

    paths shouldBe Seq(
      // (n1)-->(n2)
      Seq(graph.n1, graph.n1, graph.r1_2, graph.n2, graph.n2),
      // (n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      )
    )
  }

  test("support walk") {
    val graph = `(n1)-->(n2)`
    val nfa = `(s) ((a)--(b))+ (t)`

    val paths: Seq[Seq[Long]] = fixture()
      .withGraph(graph.graph)
      .withNfa(nfa)
      .withK(5)
      .from(graph.n1)
      .withPathMode(TraversalPathMode.Walk)
      .withMode(SearchMode.Unidirectional)
      .paths()

    paths shouldBe Seq(
      // (n1)-->(n2)
      Seq(graph.n1, graph.n1, graph.r1_2, graph.n2, graph.n2),
      // (n1)-->(n2)-->(n1)
      Seq(graph.n1, graph.n1, graph.r1_2, graph.n2, graph.n2, graph.r1_2, graph.n1, graph.n1),
      // (n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1
      ),

      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1
      )
    )
  }

  /**
   * Acyclic mode
   */
  test("acyclic does not allow node revisits in undirected traversal") {
    // Graph: (n1)-->(n2) with one relationship
    // NFA: (s) ((a)--(b))+ (t) — undirected
    // Trail: allows (n1)-->(n2) only (single rel, can't reuse)
    // Acyclic: same result (n1)-->(n2) only — backtracking would revisit n1
    val graph = `(n1)-->(n2)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(`(s) ((a)--(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .paths()

    paths shouldBe Seq(
      Seq(graph.n1, graph.n1, graph.r1_2, graph.n2, graph.n2)
    )
  }

  test("acyclic filters paths with node revisits in cycle graph") {
    // Graph: triangle (a)-->(b)-->(c)-->(a)
    val g = InMemoryGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val r_ab = g.rel(a, b)
    val r_bc = g.rel(b, c)
    g.rel(c, a)
    val graph = g.build()

    val paths = fixture()
      .withGraph(graph)
      .from(a)
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .paths()

    // Acyclic: a->b (length 1), a->b->c (length 2)
    // The full cycle a->b->c->a is forbidden because node 'a' is revisited
    paths shouldBe Seq(
      Seq(a, a, r_ab, b, b),
      Seq(a, a, r_ab, b, b, r_bc, c, c)
    )
  }

  test("acyclic with self-loops filters them out") {
    // Graph: node 'a' with two self-loops, plus a->b
    val g = InMemoryGraph.builder
    val a = g.node()
    val b = g.node()
    g.rel(a, a)
    g.rel(a, a)
    val ab = g.rel(a, b)
    val graph = g.build()

    val paths = fixture()
      .withGraph(graph)
      .from(a)
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .paths()

    paths shouldBe Seq(
      Seq(a, a, ab, b, b)
    )
  }

  test("acyclic assertExpected on cycle graph") {
    // Use assertExpected to validate the DFS reference implementation agrees
    val g = InMemoryGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    g.rel(a, b)
    g.rel(b, c)
    g.rel(c, a)
    val graph = g.build()

    fixture()
      .withGraph(graph)
      .from(a)
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .assertExpected()
  }

  test("acyclic undirected 3-node chain — source n1") {
    val graph = `(n1)-->(n2)-->(n3)`
    fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(`(s) ((a)--(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .assertExpected()
  }

  test("acyclic undirected 3-node chain — source n2") {
    val graph = `(n1)-->(n2)-->(n3)`
    fixture()
      .withGraph(graph.graph)
      .from(graph.n2)
      .withNfa(`(s) ((a)--(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .assertExpected()
  }

  test("acyclic undirected 3-node chain — source n3") {
    val graph = `(n1)-->(n2)-->(n3)`
    fixture()
      .withGraph(graph.graph)
      .from(graph.n3)
      .withNfa(`(s) ((a)--(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .assertExpected()
  }

  test("acyclic diamond graph — multiple shortest paths to same target") {
    // Graph: a→b, a→c, b→d, c→d — diamond shape
    // Both a→b→d and a→c→d are shortest acyclic paths (length 2)
    val g = InMemoryGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val d = g.node()
    g.rel(a, b)
    g.rel(a, c)
    g.rel(b, d)
    g.rel(c, d)
    val graph = g.build()

    fixture()
      .withGraph(graph)
      .from(a)
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .assertExpected()
  }

  test("acyclic source=target with directed cycle should return no non-trivial paths") {
    // Graph: a→b→c→a — triangle
    // Source = a, looking for paths ((a)-->(b))+ that end at a
    // The only way back to a is a→b→c→a which revisits a — forbidden in acyclic
    // So no paths should be returned (the NFA has separate start/final states, so no zero-length path)
    val g = InMemoryGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    g.rel(a, b)
    g.rel(b, c)
    g.rel(c, a)
    val graph = g.build()

    val paths = fixture()
      .withGraph(graph)
      .from(a)
      .into(a)
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .paths()

    paths shouldBe empty
  }

  test("acyclic multiple targets at same depth with shared signposts") {
    // Graph: a→b→c, a→b→d, d→a — shared edge a→b leads to both acyclic and cyclic paths
    // Target c at depth 2: a→b→c (acyclic ✓)
    // Target d at depth 2: a→b→d (acyclic ✓)
    // But path a→b→d→a→... would be cyclic (revisits a)
    // Verifies shared signpost a→b is not damaged by cycle detection on other branches
    val g = InMemoryGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val d = g.node()
    g.rel(a, b)
    g.rel(b, c)
    g.rel(b, d)
    g.rel(d, a) // creates potential cycle
    val graph = g.build()

    fixture()
      .withGraph(graph)
      .from(a)
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .assertExpected()
  }

  test("acyclic single node with start=final state returns single node path") {
    // Source = target with zero-length NFA (start state IS final state)
    // The single node path is valid in acyclic mode (no edges, no revisits)
    // The NFA (s) ((a)-->(b))* (t) has NJ from s→a→...→t, so zero-length path visits 3 NFA states
    val g = InMemoryGraph.builder
    val a = g.node()
    val graph = g.build()

    val paths = fixture()
      .withGraph(graph)
      .from(a)
      .withNfa(`(s) ((a)-->(b))* (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .paths()

    // Zero-length path: node a appears once per NFA state traversed (s→a→t via node juxtapositions)
    paths shouldBe Seq(Seq(a, a, a))
  }

  test("acyclic complete graph K4") {
    // 4-node complete directed graph — many potential cycles
    // Exercises the tracker with high connectivity
    val g = InMemoryGraph.builder
    val nodes = (0 until 4).map(_ => g.node())
    for (i <- nodes; j <- nodes; if i != j) g.rel(i, j)
    val graph = g.build()

    fixture()
      .withGraph(graph)
      .from(nodes.head)
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .assertExpected()
  }

  // The tests above all use NFAs whose final transition is a node juxtaposition, where the node
  // adjacent to the target IS the target. These use NFAs ending in a relationship expansion, so
  // that the target's occurrence at the end of the path is not implied by any other tracked node.

  private val `(s) ((a)-->(b))+ (c)-->(t)`: Nfa =
    nfa("s" |> ("a" --> "b" +) |> ("c" --> "t"))

  private val `(s) ((a)--(b))+ (c)--(t)`: Nfa =
    nfa("s" |> ("a" -- "b" +) |> ("c" -- "t"))

  private val `(s) ((a)--(b)){4,8} (c)--(t)`: Nfa =
    nfa("s" |> ("a" -- "b" rep (4, 8)) |> ("c" -- "t"))

  test("acyclic does not return a path that revisits the target as an inner node") {
    val graph = InMemoryGraph.fromTemplate(
      """
         (s)-[r_st]->(t)-[r_tu]->(u)-[r_uv]->(v)
                      ^                       |
                      '-----------------------'
      """
    )
    val s = graph node "s"
    val t = graph node "t"
    val u = graph node "u"
    val v = graph node "v"

    val r_st = graph rel "r_st"
    val r_tu = graph rel "r_tu"
    val r_uv = graph rel "r_uv"

    val paths = fixture()
      .withGraph(graph)
      .from(s)
      .withNfa(`(s) ((a)-->(b))+ (c)-->(t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .paths()

    // s→t→u→v→t visits the target t as an inner node and must not be returned
    paths shouldBe Seq(
      Seq(s, s, r_st, t, t, r_tu, u),
      Seq(s, s, r_st, t, t, r_tu, u, u, r_uv, v)
    )
  }

  test("acyclic rejection of a target revisit preserves a sibling path sharing its signposts") {
    // As above plus v→w: the valid path s→t→u→v→w shares all but its last signpost with the
    // rejected s→t→u→v→t, and is found at the same depth
    val graph = InMemoryGraph.fromTemplate(
      """
         (s)-[r_st]->(t)-[r_tu]->(u)-[r_uv]->(v)-[r_vw]->(w)
                      ^                       |
                      '-----------------------'
      """
    )
    val s = graph node "s"
    val t = graph node "t"
    val u = graph node "u"
    val v = graph node "v"
    val w = graph node "w"

    val r_st = graph rel "r_st"
    val r_tu = graph rel "r_tu"
    val r_uv = graph rel "r_uv"
    val r_vw = graph rel "r_vw"

    val paths = fixture()
      .withGraph(graph)
      .from(s)
      .withNfa(`(s) ((a)-->(b))+ (c)-->(t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .paths()

    paths shouldBe Seq(
      Seq(s, s, r_st, t, t, r_tu, u),
      Seq(s, s, r_st, t, t, r_tu, u, u, r_uv, v),
      Seq(s, s, r_st, t, t, r_tu, u, u, r_uv, v, v, r_vw, w)
    )
  }

  test("acyclic with a minimum repetition returns no path when every candidate cycles through the target") {
    // ANY SHORTEST ACYCLIC (:S)--{4,}()--(:A) — the only simple path from s to a is the direct
    // edge, so every candidate of 5+ hops must cycle through the source and/or the target

    val graph = InMemoryGraph.fromTemplate(
      """
         .----.      .----.
         v    |      v    |
        ()-->(s)-->(a)-->()
      """
    )
    val s = graph node "s"
    val a = graph node "a"

    val paths = fixture()
      .withGraph(graph)
      .from(s)
      .into(a)
      .withNfa(`(s) ((a)--(b)){4,8} (c)--(t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(10)
      .paths()

    paths shouldBe empty
  }

  test("acyclic assertExpected on graph with cycles through both source and target") {
    val graph = InMemoryGraph.fromTemplate(
      """
         .----.      .----.
         v    |      v    |
        ()-->(s)-->(a)-->()
      """
    )

    val s = graph node "s"

    fixture()
      .withGraph(graph)
      .from(s)
      .withNfa(`(s) ((a)--(b))+ (c)--(t)`)
      .withPathMode(TraversalPathMode.Acyclic)
      .withMaxDepth(6)
      .assertExpected()
  }

  /*******************
   * Memory tracking *
   *******************/

  test("memory tracking") {
    val graph = `(n1)-->(n2)`

    val mt = new LocalMemoryTracker()

    // TODO tests with NO_TRACKING
    // ignore the memory tracker for path tracer for the purposes of this test
    def createPathTracer(_mt: MemoryTracker, hooks: PPBFSHooks): PathTracer[TracedPath] =
      new PathTracer(
        EmptyMemoryTracker.INSTANCE,
        TraversalPathModeFactory.trailMode(EmptyMemoryTracker.INSTANCE, hooks),
        hooks
      )

    val iter = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
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
    val graph = `(n1)-->(n2)`

    val iter = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(anyDirectedPath)
      .onAssertOpen {
        throw new Exception("boom")
      }
      .build()

    the[Exception] thrownBy iter.asScala.toList should have message "boom"
  }

  /*********************************************************
   * Examples of NFAs which currently break the algorithm! *
   *********************************************************/

  ignore("single node with loop in graph, single state with loop in NFA") {
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
    val graph = InMemoryGraph.builder
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
}

object PGPathPropagatingBFSTest {

  private def anyDirectedPath(sb: PGStateBuilder): Unit = {
    val s = sb.newState(isStartState = true, isFinalState = true)
    s.addRelationshipExpansion(s, direction = Direction.OUTGOING)
  }

  private def singleRelPath(sb: PGStateBuilder): Unit = {
    sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
  }

  // noinspection TypeAnnotation
  private object `(n1)-->(n2)` {
    private val g = InMemoryGraph.builder
    val n1 = g.node()
    val n2 = g.node()
    val r1_2 = g.rel(n1, n2)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)<--(b)-->(c)` {
    private val g = InMemoryGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val ba = g.rel(b, a)
    val bc = g.rel(b, c)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(n1)-->(n2)-->(n3)` {
    private val g = InMemoryGraph.builder
    val n1 = g.node()
    val n2 = g.node()
    val n3 = g.node()
    val r1_2 = g.rel(n1, n2)
    val r2_3 = g.rel(n2, n3)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)-->(b)<--(c)` {
    private val g = InMemoryGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val ab = g.rel(a, b)
    val cb = g.rel(c, b)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)<--(b)-->(a)` {
    private val g = InMemoryGraph.builder
    val a = g.node()
    val b = g.node()
    val ba1 = g.rel(b, a)
    val ba2 = g.rel(b, a)
    val graph = g.build()
  }

}
