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
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class TriadicSelectionTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private def basicQuery(positivePredicate: Boolean): LogicalQuery = {
    new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .triadicSelection(positivePredicate, "x", "y", "z")
      .|.expandAll("(y)-->(z)")
      .|.argument("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()
  }

  private def basicQueryWithApply(positivePredicate: Boolean): LogicalQuery = {
    new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.triadicSelection(positivePredicate, "x", "y", "z")
      .|.|.expandAll("(y)-->(z)")
      .|.|.argument("x", "y")
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()
  }

  private def beBasicQueryColumns: RuntimeResultMatcher = beColumns("x", "y", "z")

  private def expectedResultForBasicQuery(nodes: Seq[Node], positivePredicate: Boolean): Seq[Array[Any]] = {
    def friends(n: Node): Set[Node] =
      n.getRelationships(Direction.OUTGOING).asScala.map(_.getEndNode).toSet

    for {
      x <- nodes
      xFriends = friends(x)
      y <- xFriends
      yFriends = friends(y)
      z <- if (positivePredicate) yFriends.intersect(xFriends) else yFriends.diff(xFriends)
    } yield Array[Any](x, y, z)
  }

  private def basicQueryAndExpectedResult(nodes: Seq[Node], positivePredicate: Boolean) =
    (basicQuery(positivePredicate), expectedResultForBasicQuery(nodes, positivePredicate))

  private def basicQueryWithApplyAndExpectedResult(nodes: Seq[Node], positivePredicate: Boolean) =
    (basicQueryWithApply(positivePredicate), expectedResultForBasicQuery(nodes, positivePredicate))

  test("empty input gives empty output with negative predicate") {
    val logicalQuery = basicQuery(positivePredicate = false)
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beBasicQueryColumns.withNoRows()
  }

  test("empty input gives empty output with positive predicate") {
    val logicalQuery = basicQuery(positivePredicate = true)
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beBasicQueryColumns.withNoRows()
  }

  test("friend of a friend that is not already a friend") {
    val (nodes, _, _) = given {
      nestedStarGraph(3, 3, "C", "R")
    }

    val (logicalQuery, expectedResult) = basicQueryAndExpectedResult(nodes, positivePredicate = false)
    val runtimeResult = execute(logicalQuery, runtime)
    expectedResult shouldNot be(empty)
    runtimeResult should beBasicQueryColumns.withRows(expectedResult)
  }

  test("empty output when no friend of a friend that is not already a friend") {
    given {
      val size = 100
      val nodes = nodeGraph(size)
      val rels = for {
        n <- 0 until size
        m <- 0 until size
      } yield (n, m, "FRIEND")
      connect(nodes, rels)
      nodes
    }

    val logicalQuery = basicQuery(positivePredicate = false)
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beBasicQueryColumns.withNoRows()
  }

  test("mutual friends") {
    val nodes = given {
      val size = 100
      val nodes = nodeGraph(size)
      val rels = for {
        n <- 0 until size
        m <- (n - 2) to (n + 2) if m != n && m >= 0 && m < size
      } yield (n, m, "FRIEND")

      connect(nodes, rels)
      nodes
    }

    val (logicalQuery, expectedResult) = basicQueryAndExpectedResult(nodes, positivePredicate = true)
    val runtimeResult = execute(logicalQuery, runtime)
    expectedResult shouldNot be(empty)
    runtimeResult should beBasicQueryColumns.withRows(expectedResult)
  }

  test("empty output when no mutual friends") {
    given {
      nestedStarGraph(3, 3, "C", "R")
    }

    val logicalQuery = basicQuery(positivePredicate = true)
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beBasicQueryColumns.withNoRows()
  }

  test("empty input gives empty output with negatitve predicate under apply") {
    val logicalQuery = basicQueryWithApply(positivePredicate = false)
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beBasicQueryColumns.withNoRows()
  }

  test("empty input gives empty output with positive predicate under apply") {
    val logicalQuery = basicQueryWithApply(positivePredicate = true)
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beBasicQueryColumns.withNoRows()
  }

  test("friend of a friend that is not already a friend under apply") {
    val (nodes, _, _) = given {
      nestedStarGraph(3, 3, "C", "R")
    }

    val (logicalQuery, expectedResult) = basicQueryWithApplyAndExpectedResult(nodes, positivePredicate = false)
    val runtimeResult = execute(logicalQuery, runtime)
    expectedResult shouldNot be(empty)
    runtimeResult should beBasicQueryColumns.withRows(expectedResult)
  }

  test("empty output when no friend of a friend that is not already a friend under apply") {
    given {
      val size = 100
      val nodes = nodeGraph(size)
      val rels = for {
        n <- 0 until size
        m <- 0 until size
      } yield (n, m, "FRIEND")
      connect(nodes, rels)
      nodes
    }

    val logicalQuery = basicQueryWithApply(positivePredicate = false)
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beBasicQueryColumns.withNoRows()
  }

  test("mutual friends under apply") {
    val nodes = given {
      val size = 100
      val nodes = nodeGraph(size)
      val rels = for {
        n <- 0 until size
        m <- (n - 2) to (n + 2) if m != n && m >= 0 && m < size
      } yield (n, m, "FRIEND")

      connect(nodes, rels)
      nodes
    }

    val (logicalQuery, expectedResult) = basicQueryWithApplyAndExpectedResult(nodes, positivePredicate = true)
    val runtimeResult = execute(logicalQuery, runtime)
    expectedResult shouldNot be(empty)
    runtimeResult should beBasicQueryColumns.withRows(expectedResult)
  }

  test("empty output when no mutual friends under apply") {
    given {
      nestedStarGraph(3, 3, "C", "R")
    }

    val logicalQuery = basicQueryWithApply(positivePredicate = true)
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beBasicQueryColumns.withNoRows()
  }

  test("works with limit") {
    given {
      nestedStarGraph(2, 5, "C", "R")
    }

    val limit = 7

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .limit(limit)
      .triadicSelection(positivePredicate = false, "x", "y", "z")
      .|.expandAll("(y)-->(z)")
      .|.argument("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y", "z").withRows(rowCount(limit))
  }

  test("works with aggregation") {
    given {
      chainGraphs(sizeHint, "A", "B")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .triadicSelection(positivePredicate = false, "x", "y", "z")
      .|.expandAll("(y)-->(z)")
      .|.argument("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withSingleRow(sizeHint)
  }

  test("works with a single variable") {
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .triadicSelection(positivePredicate = true, "n", "n", "n")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withRows(nodes.map(Array(_)))
  }

  test("works with limit on RHS") {
    val ringSize = 5
    given {
      nestedStarGraph(2, ringSize, "C", "R")
    }

    val limit = 2
    val expectedCount = math.min(ringSize, limit) * ringSize

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .triadicSelection(positivePredicate = false, "x", "y", "z")
      .|.limit(limit)
      .|.expandAll("(y)<--(z)")
      .|.argument("x", "y")
      .expandAll("(x)<--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y", "z").withRows(rowCount(expectedCount))
  }

  test("works with apply on RHS") {
    given {
      chainGraphs(sizeHint, "A", "B")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .triadicSelection(positivePredicate = false, "x", "y", "z")
      .|.apply()
      .|.|.expandAll("(y)-->(z)")
      .|.|.argument("x", "y")
      .|.argument("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y", "z").withRows(rowCount(sizeHint))
  }

  test("should handle repeated pattern with loop") {
    val node = given {
      val node = tx.createNode()
      node.createRelationshipTo(node, RelationshipType.withName("R"))
      (1 to sizeHint).foreach(_ => {
        node.createRelationshipTo(tx.createNode(), RelationshipType.withName("R"))
        node.createRelationshipTo(tx.createNode(), RelationshipType.withName("R"))
      })
      node
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.triadicSelection(positivePredicate = false, "b", "c", "d")
      .|.|.expand("(c)-->(d)")
      .|.|.argument("c")
      .|.expand("(b)-->(c)")
      .|.argument("b")
      .expand("(a)-->(b)")
      .nodeByIdSeek("a", Set.empty, node.getId)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a", "b", "c").withNoRows()
  }
}
