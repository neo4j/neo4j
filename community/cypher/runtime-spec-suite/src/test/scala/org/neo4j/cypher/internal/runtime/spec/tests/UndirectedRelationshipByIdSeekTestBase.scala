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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.RelationshipType

import scala.util.Random

abstract class UndirectedRelationshipByIdSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private val random = new Random(77)

  test("should find single relationship") {
    // given
    val (_, relationships) = givenGraph { circleGraph(17) }
    val relToFind = relationships(random.nextInt(relationships.length))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, relToFind.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(Seq(
      Array(relToFind, relToFind.getStartNode, relToFind.getEndNode),
      Array(relToFind, relToFind.getEndNode, relToFind.getStartNode)
    ))
  }

  test("should find by floating point") {
    // given
    val (_, Seq(rel, _)) = givenGraph { circleGraph(2) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, rel.getId.toDouble)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(rel, rel)))
  }

  test("should not find non-existing relationship") {
    // given
    val (_, relationships) = givenGraph { circleGraph(17) }
    val toNotFind = relationships.map(_.getId).max + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, toNotFind)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withNoRows()
  }

  test("should find multiple relationships") {
    // given
    val (_, relationships) = givenGraph { circleGraph(sizeHint) }
    val toFind = (1 to 5).map(_ => relationships(random.nextInt(relationships.length)))
    restartTx()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, toFind.map(_.getId): _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = toFind.flatMap(r => {
      val rel = tx.getRelationshipById(r.getId)
      Seq(Array(rel, rel.getStartNode, rel.getEndNode), Array(rel, rel.getEndNode, rel.getStartNode))
    })
    runtimeResult should beColumns("r", "x", "y").withRows(expected)
  }

  test("should find some relationships and not others") {
    // given
    val (_, relationships) = givenGraph { circleGraph(sizeHint) }
    val toFind = (1 to 5).map(_ => relationships(random.nextInt(relationships.length)))
    val toNotFind1 = relationships.map(_.getId).max + 1
    val toNotFind2 = toNotFind1 + 1
    val relationshipsToLookFor = toNotFind1 +: toFind.map(_.getId) :+ toNotFind2
    restartTx()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, relationshipsToLookFor: _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = toFind.flatMap(r => {
      val rel = tx.getRelationshipById(r.getId)
      Seq(Array(rel, rel.getStartNode, rel.getEndNode), Array(rel, rel.getEndNode, rel.getStartNode))
    })
    runtimeResult should beColumns("r", "x", "y").withRows(expected)
  }

  test("should handle relById + filter") {
    // given
    val (_, relationships) = givenGraph { circleGraph(sizeHint) }
    val toSeekFor = (1 to 5).map(_ => relationships(random.nextInt(relationships.length)))
    val toFind = toSeekFor(random.nextInt(toSeekFor.length))
    restartTx()

    val attachedToFind = tx.getRelationshipById(toFind.getId)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter(s"id(r) = ${attachedToFind.getId}")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, toSeekFor.map(_.getId): _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(Seq(
      Array(attachedToFind, attachedToFind.getStartNode, attachedToFind.getEndNode),
      Array(attachedToFind, attachedToFind.getEndNode, attachedToFind.getStartNode)
    ))
  }

  test("should handle limit + sort") {
    val (nodes, relationships) = givenGraph {
      circleGraph(sizeHint, "A")
    }
    val limit = 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.limit(limit)
      .|.sort("r ASC", "x ASC")
      .|.undirectedRelationshipByIdSeek("r", "x", "y", Set("a1"), relationships.head.getId)
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(nodes.map(_ => Array[Any](nodes.head)))
  }

  test("should handle continuation from single undirectedRelationshipByIdSeek") {
    // given
    val nodesPerLabel = sizeHint / 4
    val (r, nodes) = givenGraph {
      val (_, _, rs, _) = bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
      val r = rs.head
      val nodes = Seq(r.getStartNode, r.getEndNode)
      (r, nodes)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, r.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    val expected = for {
      Seq(x, y) <- nodes.permutations.toSeq
      _ <- 0 until nodesPerLabel
    } yield Array(r, x, y)

    runtimeResult should beColumns("r", "x", "y").withRows(expected)
  }

  test("should handle continuation from multiple undirectedRelationshipByIdSeek") {
    // given
    val nodesPerLabel = 20
    val (rs, nodes) = givenGraph {
      val (_, _, rs, _) = bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
      val nodes = rs.map(r => r -> Seq(r.getStartNode, r.getEndNode)).toMap
      (rs, nodes)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, rs.map(_.getId): _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    val expected = for {
      r <- rs
      Seq(x, y) <- nodes(r).permutations.toSeq
      _ <- 0 until nodesPerLabel
    } yield Array(r, x, y)

    runtimeResult should beColumns("r", "x", "y").withRows(expected)
  }

  test("should only find loop once") {
    // given
    val relToFind = givenGraph {
      val a = tx.createNode()
      a.createRelationshipTo(a, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, relToFind.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(Seq(
      Array(relToFind, relToFind.getStartNode, relToFind.getEndNode)
    ))
  }

  test("should only find loop once, many ids") {
    // given
    val relToFind = givenGraph {
      val a = tx.createNode()
      a.createRelationshipTo(a, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, Seq.fill(10)(relToFind.getId): _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(Seq.fill(10)(
      Array(relToFind, relToFind.getStartNode, relToFind.getEndNode)
    ))
  }

  test(s"should produce all expected rows from single undirectedRelationByIdSeek") {
    val (_, _, abs, bas) = givenGraph(bidirectionalBipartiteGraph(5, "A", "B", "AB", "BA"))
    val expected = (abs ++ abs ++ bas ++ bas).map(r => Array(r))

    val query = new LogicalQueryBuilder(this)
      .produceResults("r")
      .semiApply()
      .|.expand("(c)-[:BA]->(d)")
      .|.undirectedRelationshipByIdSeek("unused1", "unused2", "c", Set(), abs.head.getId)
      .allRelationshipsScan("(a)-[r]-(b)")
      .build()

    val actual = execute(query, runtime)
    actual should beColumns("r").withRows(inAnyOrder(expected))
  }
}
