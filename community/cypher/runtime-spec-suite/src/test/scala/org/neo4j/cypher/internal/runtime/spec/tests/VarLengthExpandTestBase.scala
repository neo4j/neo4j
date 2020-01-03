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

import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{INCOMING, OUTGOING}
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.graphdb.{Node, RelationshipType}

import scala.util.Random

abstract class VarLengthExpandTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("simple var-length-expand") {
    // given
    val n = sizeHint / 6
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.startNode, path.take(length).endNode())

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("var-length-expand with bound relationships") {
    // given
    val paths = given { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield {
        val pathPrefix = path.take(length)
        Array(pathPrefix.startNode, pathPrefix.relationships(), pathPrefix.endNode())
      }

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand on lollipop graph") {
    // given
    val (Seq(n1, n2, n3), Seq(r1, r2, r3)) = given { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      Array(
        Array(n1, Array.empty, n1),
        Array(n1, Array(r1), n2),
        Array(n1, Array(r1, r3), n3),
        Array(n1, Array(r2), n2),
        Array(n1, Array(r2, r3), n3))

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand with max length") {
    // given
    val (Seq(n1, n2, _), Seq(r1, r2, _)) = given { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*..1]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      Array(
        Array(n1, Array.empty, n1),
        Array(n1, Array(r1), n2),
        Array(n1, Array(r2), n2))

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand with min and max length") {
    // given
    val paths = given { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*2..4]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 2 to 4
      } yield {
        val pathPrefix = path.take(length)
        Array(pathPrefix.startNode, pathPrefix.relationships(), pathPrefix.endNode())
      }

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand with length 0") {
    // given
    val (nodes, _) = given { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[r*0]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  // VAR EXPAND INTO

  test("simple var-length-expand-into") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputColumns(4, n/4, i => paths(i).startNode, i => paths(i).endNode())

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected: IndexedSeq[Array[Node]] = paths.map(p => Array(p.startNode, p.endNode()))

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("var-length-expand-into with non-matching end") {
    // given
    val (paths, nonMatching) = given {
      val paths = chainGraphs(1, "TO", "TO", "TO", "TOO", "TO")
      val nonMatching = nodeGraph(1).head
      (paths, nonMatching)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val input = inputValues(Array(paths.head.startNode, nonMatching))
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("var-length-expand-into with end not being a node") {
    // given
    val paths = given { chainGraphs(1, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto)
      .input(variables = Seq("x", "y"))
      .build()

    val input = inputValues(Array(paths.head.startNode, 42))
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("var-length-expand-into with end bound to mix of matching and non-matching nodes") {
    // given
    val n = closestMultipleOf(sizeHint / 5, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }
    val random = new Random

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    case class InputDef(start: Node, end: Node, matches: Boolean)
    val input =
      for (i <- 0 until n) yield {
        val matches = random.nextBoolean()
        val endNode =
          if (matches) {
            paths(i).endNode()
          } else {
            val not_i = (i + 1 + random.nextInt(paths.size - 1)) % n
            paths(not_i).endNode()
          }
        InputDef(paths(i).startNode, endNode, matches)
      }

    val runtimeResult = execute(logicalQuery, runtime, inputColumns(4, n/4, i => input(i).start, i => input(i).end))

    // then
    val expected: IndexedSeq[Array[Node]] =
      input.filter(_.matches).map(p => Array(p.start, p.end))

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("var-length-expand-into on lollipop graph") {
    // given
    val (Seq(n1, _, n3), Seq(r1, r2, r3)) = given { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)", expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(n1, n3)))

    // then
    val expected =
      Array(
        Array(n1, Array(r1, r3), n3),
        Array(n1, Array(r2, r3), n3))

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand-into with max length") {
    // given
    val (n1, n3, r4) = given {
      val (Seq(n1, _, n3), _) = lollipopGraph()
      val r4 = n1.createRelationshipTo(n3, RelationshipType.withName("R"))
      (n1, n3, r4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*..1]->(y)", expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(n1, n3)))

    // then
    val expected = Array(Array(n1, Array(r4), n3))

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand-into with min length") {
    // given
    val (n1, n3, r1, r2, r3, r4) = given {
      val (Seq(n1, _, n3), Seq(r1, r2, r3)) = lollipopGraph()
      val r4 = tx.getNodeById(n1.getId).createRelationshipTo(tx.getNodeById(n3.getId), RelationshipType.withName("R"))
      (n1, n3, r1, r2, r3, r4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*2..2]->(y)", expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(n1, n3)))

    // then
    val expected = Array(
      Array(n1, Array(r1, r3), n3),
      Array(n1, Array(r2, r3), n3))

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  // PATH PROJECTION

  test("should project (x)-[r*]->(y) correctly when from matching from x") {
    // given
    val paths = given { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())):_*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)", projectedDir = OUTGOING, expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array(p.startNode, p.relationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (x)-[r*]->(y) correctly when from matching from y") {
    // given
    val paths = given { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())):_*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(y)<-[r*]-(x)", projectedDir = OUTGOING, expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array(p.startNode, p.relationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (y)<-[r*]-(x) correctly when from matching from x") {
    // given
    val paths = given { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())):_*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)", projectedDir = INCOMING, expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array(p.startNode, p.reverseRelationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (y)<-[r*]-(x) correctly when from matching from y") {
    // given
    val paths = given { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())):_*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(y)<-[r*]-(x)", projectedDir = INCOMING, expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array(p.startNode, p.reverseRelationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (x)-[r*]-(y) correctly when from matching from x") {
    // given
    val paths = given { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())):_*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]-(y)", projectedDir = OUTGOING, expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array(p.startNode, p.relationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (x)-[r*]-(y) correctly when from matching from y") {
    // given
    val paths = given { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())):_*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(y)-[r*]-(x)", projectedDir = INCOMING, expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array(p.startNode, p.relationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  // NULL INPUT

  test("should handle null from-node") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]-(y)")
      .input(nodes = Seq("x"))
      .build()

    val input = inputValues(Array(Array[Any](null)):_*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  test("should handle null from-node without overwriting to-node in expand into") {
    // given
    val n1 = given { nodeGraph(1).head }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]-(y)", expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val input = inputValues(Array(Array[Any](null, n1)):_*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  // EXPANSION FILTERING, DIRECTION

  test("should filter on outgoing direction") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r*1..2]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sb1),
      Array(g.sa1),
      Array(g.middle),
      Array(g.sb2),
      Array(g.middle),
      Array(g.sc3),
      Array(g.ea1),
      Array(g.eb1),
      Array(g.ec1)
    ))
  }

  test("should filter on incoming direction") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)<-[r*1..2]-(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sc1),
      Array(g.sc2)
    ))
  }

  test("should expand on BOTH direction") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r*1..2]-(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sb1), // outgoing only
      Array(g.sa1),
      Array(g.middle),
      Array(g.sb2),
      Array(g.middle),
      Array(g.sc3),
      Array(g.ea1),
      Array(g.eb1),
      Array(g.ec1),
      Array(g.sc1), // incoming only
      Array(g.sc2),
      Array(g.sb2), // mixed
      Array(g.sa1),
      Array(g.end)
    ))
  }

  // EXPANSION FILTERING, RELATIONSHIP TYPE

  test("should filter on relationship type A") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r:A*1..2]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.middle),
      Array(g.middle),
      Array(g.sc3),
      Array(g.ea1),
      Array(g.ec1)
    ))
  }

  test("should filter on relationship type B") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r:B*1..2]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sb1),
      Array(g.sb2)
    ))
  }

  // EXPANSION FILTERING, NODE AND RELATIONSHIP PREDICATE

  test("should filter on node predicate") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r:*1..2]-(y)", nodePredicate = Predicate("n", "id(n) <> "+g.middle.getId))
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.sb1),
      Array(g.sb2),
      Array(g.sc1),
      Array(g.sc2)
    ))
  }

  test("should filter on node predicate on first node") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r:*1..2]-(y)", nodePredicate = Predicate("n", "id(n) <> "+g.start.getId))
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on node predicate on first node from reference") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(X)-[r:*1..2]-(y)", nodePredicate = Predicate("n", "id(n) <> "+g.start.getId))
      .projection("x AS X")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on relationship predicate") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r:*1..2]->(y)", relationshipPredicate = Predicate("r", "id(r) <> "+g.startMiddle.getId))
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.middle),
      Array(g.sb1),
      Array(g.sb2)
    ))
  }

  test("should filter on node and relationship predicate") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r:*2..2]-(y)",
        nodePredicate = Predicate("n", "id(n) <> "+g.sa1.getId),
        relationshipPredicate = Predicate("r", "id(r) <> "+g.startMiddle.getId))
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sc2),
      Array(g.sb2)
    ))
  }

  test("should handle predicate accessing start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", nodePredicate = Predicate("n", "'START' IN labels(x)"))
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n/4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand into with predicate accessing end node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto, nodePredicate = Predicate("n", "'END' IN labels(y)"))
      .input(nodes = Seq("x", "y"))
      .build()

    val input = inputColumns(4, n/4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected: IndexedSeq[Array[Node]] = paths.map(p => Array(p.startNode, p.endNode()))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing start node when reference") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", nodePredicate = Predicate("n", "'START' IN labels(x)"))
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(4, n/4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
    for {
      path <- paths
      length <- 0 to 5
    } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand into with predicate accessing end node when reference") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto, nodePredicate = Predicate("n", "'END' IN labels(y)"))
      .input(variables = Seq("x", "y"))
      .build()

    val input = inputColumns(4, n/4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected: IndexedSeq[Array[Node]] = paths.map(p => Array(p.startNode, p.endNode()))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing reference in context") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", nodePredicate = Predicate("n", "id(n) >= zero"))
      .projection("0 AS zero")
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n/4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing node in context") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", nodePredicate = Predicate("n", "id(other) >= 0"))
      .projection("0 AS zero")
      .input(nodes = Seq("x", "other"))
      .build()

    val input = inputColumns(4, n/4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  // HELPERS

  private def closestMultipleOf(sizeHint: Int, div: Int) = (sizeHint / div) * div
}
