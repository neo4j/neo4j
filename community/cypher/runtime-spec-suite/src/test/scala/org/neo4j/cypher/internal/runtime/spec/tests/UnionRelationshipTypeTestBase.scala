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
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.RelationshipType

abstract class UnionRelationshipTypeTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should do directed scan of all relationships with types") {
    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]->(y)", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withRows(singleColumn(rels))
  }

  test("should do undirected scan of all relationships with types") {
    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]-(y)", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withRows(singleColumn(rels.flatMap(r => Seq(r, r))))
  }

  test("should do directed scan of all relationships of a label in ascending order") {
    // parallel does not maintain order
    assume(!isParallel)

    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]->(y)", IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withRows(singleColumnInOrder(rels.sortBy(_.getId)))
  }

  test("should do undirected scan of all relationships of a label in ascending order") {
    // parallel does not maintain order
    assume(!isParallel)

    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]-(y)", IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withRows(singleColumnInOrder(rels.sortBy(_.getId).flatMap(r => Seq(r, r))))
  }

  test("should do directed scan of all relationships of a label in descending order") {
    // parallel does not maintain order
    assume(!isParallel)
    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]->(y)", IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withRows(singleColumnInOrder(rels.sortBy(_.getId * -1)))
  }

  test("should do undirected scan of all relationships of a label in descending order") {
    // parallel does not maintain order
    assume(!isParallel)

    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]-(y)", IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withRows(singleColumnInOrder(rels.sortBy(_.getId * -1).flatMap(r => Seq(r, r))))
  }

  test("should scan empty graph directed") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]->(y)", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should scan empty graph undirected") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]-(y)", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle multiple directed scans") {
    // given
    val (_, rels) = givenGraph { circleGraph(10, "A", 1) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.unionRelationshipTypesScan("(x3)-[r3:A|B|C]->(y3)", IndexOrderNone)
      .apply()
      .|.unionRelationshipTypesScan("(x2)-[r2:A|B|C]->(y2)", IndexOrderNone)
      .unionRelationshipTypesScan("(x1)-[r1:A|B|C]->(y1)", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { r1 <- rels; r2 <- rels; r3 <- rels } yield Array(r1, r2, r3)
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected)
  }

  test("should handle multiple undirected scans") {
    // given
    val (_, rels) = givenGraph { circleGraph(10, "A", 1) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.unionRelationshipTypesScan("(x3)-[r3:A|B|C]-(y3)", IndexOrderNone)
      .apply()
      .|.unionRelationshipTypesScan("(x2)-[r2:A|B|C]-(y2)", IndexOrderNone)
      .unionRelationshipTypesScan("(x1)-[r1:A|B|C]-(y1)", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- rels.flatMap(r => Seq(r, r)); r2 <- rels.flatMap(r => Seq(r, r)); r3 <- rels.flatMap(r => Seq(r, r))
    } yield Array(r1, r2, r3)
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected)
  }

  test("should handle non-existing types, directed") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]->(y)", IndexOrderNone)
      .build()

    // empty db
    val executablePlan = buildPlan(logicalQuery, runtime)
    execute(executablePlan) should beColumns("x").withNoRows()

    // CREATE A
    givenGraph(circleGraph(sizeHint, "A", 1))
    execute(executablePlan) should beColumns("x").withRows(rowCount(sizeHint))

    // CREATE B
    givenGraph(circleGraph(sizeHint, "B", 1))
    execute(executablePlan) should beColumns("x").withRows(rowCount(2 * sizeHint))

    // CREATE C
    givenGraph(circleGraph(sizeHint, "C", 1))
    execute(executablePlan) should beColumns("x").withRows(rowCount(3 * sizeHint))
  }

  test("should handle non-existing types, undirected") {
    // given
    val batchSize = sizeHint / 10
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]-(y)", IndexOrderNone)
      .build()

    // empty db
    val executablePlan = buildPlan(logicalQuery, runtime)
    execute(executablePlan) should beColumns("x").withNoRows()

    // CREATE A
    givenGraph(circleGraph(batchSize, "A", 1))
    execute(executablePlan) should beColumns("x").withRows(rowCount(2 * batchSize))

    // CREATE B
    givenGraph(circleGraph(batchSize, "B", 1))
    execute(executablePlan) should beColumns("x").withRows(rowCount(4 * batchSize))

    // CREATE C
    givenGraph(circleGraph(batchSize, "C", 1))
    execute(executablePlan) should beColumns("x").withRows(rowCount(6 * batchSize))
  }

  test("directed scan on the RHS of apply") {
    // given
    val (aRels, bRels, cRels, dRels) = givenGraph {
      val (_, aRels) = circleGraph(10, "A", 1)
      val (_, bRels) = circleGraph(10, "B", 1)
      val (_, cRels) = circleGraph(10, "C", 1)
      val (_, dRels) = circleGraph(10, "C", 1)

      (aRels, bRels, cRels, dRels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2")
      .apply()
      .|.unionRelationshipTypesScan("(x2)-[r2:C|D]->(y2)", IndexOrderNone, "x1", "r1", "y1")
      .unionRelationshipTypesScan("(x1)-[r1:A|B]->(y1)", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { r1 <- aRels ++ bRels; r2 <- cRels ++ dRels } yield Array(r1, r2)
    runtimeResult should beColumns("r1", "r2").withRows(expected)
  }

  test("undirected scan on the RHS of apply") {
    // given
    val (aRels, bRels, cRels, dRels) = givenGraph {
      val (_, aRels) = circleGraph(10, "A", 1)
      val (_, bRels) = circleGraph(10, "B", 1)
      val (_, cRels) = circleGraph(10, "C", 1)
      val (_, dRels) = circleGraph(10, "C", 1)

      (aRels, bRels, cRels, dRels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2")
      .apply()
      .|.unionRelationshipTypesScan("(x2)-[r2:C|D]-(y2)", IndexOrderNone, "x1", "r1", "y1")
      .unionRelationshipTypesScan("(x1)-[r1:A|B]-(y1)", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- aRels.flatMap(r => Seq(r, r)) ++ bRels.flatMap(r => Seq(r, r))
      r2 <- cRels.flatMap(r => Seq(r, r)) ++ dRels.flatMap(r => Seq(r, r))
    } yield Array(r1, r2)
    runtimeResult should beColumns("r1", "r2").withRows(expected)
  }

  test("scan should get source, target and type") {
    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "t")
      .projection("x AS x", "y AS y", "type(r) AS t")
      .unionRelationshipTypesScan("(x)-[r:A|B|C]->(y)", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels.map(r => Array[Any](r.getStartNode, r.getEndNode, r.getType.name()))
    runtimeResult should beColumns("x", "y", "t").withRows(expected)
  }

  test("undirected scans only find loop once") {
    val rel = givenGraph {
      val a = tx.createNode()
      a.createRelationshipTo(a, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .unionRelationshipTypesScan("(n)-[r:R|S|T]-(m)", IndexOrderNone)
      .build()

    execute(logicalQuery, runtime) should beColumns("r").withSingleRow(rel)
  }
}
