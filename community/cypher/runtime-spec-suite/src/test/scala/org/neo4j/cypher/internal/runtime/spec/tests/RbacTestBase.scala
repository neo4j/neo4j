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

abstract class RbacTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  val username = "customUser"
  val password = "password1234"
  val roleName = "customRole"

  def setupCustomUser(username: String = username, password: String = password, rolename: String = roleName) = {
    systemDb.executeTransactionally(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    systemDb.executeTransactionally(s"CREATE ROLE $rolename IF NOT EXISTS")
    systemDb.executeTransactionally(s"GRANT ROLE $rolename TO $username")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    setupCustomUser()
  }

  test("relationshipTypeScan should traverse granted labels") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO $roleName")
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES A, B, D TO $roleName")

    val (bNodes, cNodes, dNodes) = givenGraph {
      val (nodes, rels) = starGraphMultiLabel(sizeHint, "A", Seq("B", "C", "D"))
      val nMap = nodes.groupBy(n => n.getLabels.iterator().next().name())
      (nMap("B"), nMap("C"), nMap("D"))
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .relationshipTypeScan("()-[:R]->(n)")
      .build()

    val result = executeAs(query, runtime, username, password)

    val expected = bNodes ++ dNodes
    result should beColumns("n").withRows(singleColumn(expected))
  }

  test("relationshipTypeScan should traverse granted labels in incoming direction") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO $roleName")
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES A,B, D TO $roleName")

    val (bNodes, cNodes, dNodes) = givenGraph {
      val (nodes, rels) = starGraphMultiLabel(sizeHint, "A", Seq("B", "C", "D"))
      val nMap = nodes.groupBy(n => n.getLabels.iterator().next().name())
      (nMap("B"), nMap("C"), nMap("D"))
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .relationshipTypeScan("(n)<-[:R]-()")
      .build()

    val result = executeAs(query, runtime, username, password)

    val expected = bNodes ++ dNodes
    result should beColumns("n").withRows(singleColumn(expected))
  }

  test("allRelationshipScan should traverse granted labels") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO $roleName")
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES A, B, D TO $roleName")

    val (bNodes, cNodes, dNodes) = givenGraph {
      val (nodes, rels) = starGraphMultiLabel(sizeHint, "A", Seq("B", "C", "D"))
      val nMap = nodes.groupBy(n => n.getLabels.iterator().next().name())
      (nMap("B"), nMap("C"), nMap("D"))
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .allRelationshipsScan("()-[:R]->(n)")
      .build()

    val result = executeAs(query, runtime, username, password)

    val expected = bNodes ++ dNodes
    result should beColumns("n").withRows(singleColumn(expected))
  }

  test("GRANT READ should allow properties on nodes") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")
    systemDb.executeTransactionally(s"GRANT READ {a,b,c} ON GRAPH * NODES A TO $roleName")
    systemDb.executeTransactionally(s"GRANT READ {b} ON GRAPH * NODES B TO $roleName")
    givenGraph {
      nodePropertyGraph(sizeHint, { case i: Int => Map("a" -> i, "c" -> 3) }, "A")
      nodePropertyGraph(sizeHint, { case i: Int => Map("b" -> i, "c" -> 3) }, "B")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .projection("n.a AS a", "n.b AS b", "n.c AS c")
      .allNodeScan("n")
      .build()

    val result = executeAs(query, runtime, username, password)

    val aProps = for { i <- 0 until sizeHint } yield Array(i, null, 3)
    val bProps = for { i <- 0 until sizeHint } yield Array(null, i, null)
    val expected = aProps ++ bProps
    result should beColumns("a", "b", "c").withRows(expected)
  }

  test("GRANT MATCH should allow labels and properties on nodes") {
    systemDb.executeTransactionally(s"GRANT MATCH { a, b, c } ON GRAPH * NODES A, C to $roleName")
    givenGraph {
      nodePropertyGraph(sizeHint, { case i: Int => Map("a" -> i, "d" -> 0) }, "A")
      nodePropertyGraph(sizeHint, { case i: Int => Map("b" -> i, "d" -> 0) }, "B")
      nodePropertyGraph(sizeHint, { case i: Int => Map("c" -> i, "d" -> 0) }, "C")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("`n.a`", "`n.b`", "`n.c`", "`n.d`")
      .projection("n.a as `n.a`", "n.b as `n.b`", "n.c as `n.c`", "n.d as `n.d`")
      .allNodeScan("n")
      .build()

    val result = executeAs(query, runtime, username, password)

    val aProps = for { i <- 0 until sizeHint } yield Array(i, null, null, null)
    val cProps = for { i <- 0 until sizeHint } yield Array(null, null, i, null)
    val expected = aProps ++ cProps
    result should beColumns("n.a", "n.b", "n.c", "n.d").withRows(expected)
  }

  test("nodeCountFromCountStore should include a granted label") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES A,B,C TO $roleName")
    givenGraph {
      nodeGraph(sizeHint, "A") // counted, A is granted
      nodeGraph(sizeHint, "A", "SECRET") // counted, A is granted
      nodeGraph(sizeHint, "B", "SECRET") // not counted
      nodeGraph(sizeHint, "C") // not counted
      nodeGraph(sizeHint, "SECRET") // not counted
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(Some("A")))
      .build()

    val result = executeAs(query, runtime, username, password)

    result should beColumns("x").withSingleRow(sizeHint * 2)
  }

  test("nodeCountFromCountStore should include all granted labels when counting everything") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES A,B,C TO $roleName")
    givenGraph {
      nodeGraph(sizeHint, "A") // counted, A is granted
      nodeGraph(sizeHint, "A", "SECRET") // counted, A is granted
      nodeGraph(sizeHint, "B", "SECRET") // counted, B is granted
      nodeGraph(sizeHint, "C") // counted, C is granted
      nodeGraph(sizeHint, "SECRET") // not counted, SECRET is not granted
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(None))
      .build()

    val result = executeAs(query, runtime, username, password)

    result should beColumns("x").withSingleRow(sizeHint * 4)
  }

  test("nodeCountFromCountStore should exclude denied labels") {
    systemDb.executeTransactionally(s"DENY TRAVERSE ON GRAPH * NODES SECRET TO $roleName")
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES A,B,C TO $roleName")
    givenGraph {
      nodeGraph(sizeHint, "A") // counted
      nodeGraph(sizeHint, "A", "SECRET") // not counted, SECRET is denied
      nodeGraph(sizeHint, "B", "SECRET") // not counted
      nodeGraph(sizeHint, "C") // not counted
      nodeGraph(sizeHint, "SECRET") // not counted
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(Some("A")))
      .build()

    val result = executeAs(query, runtime, username, password)

    result should beColumns("x").withSingleRow(sizeHint)
  }

  test("nodeCountFromCountStore should exclude denied labels when counting everything") {
    systemDb.executeTransactionally(s"DENY TRAVERSE ON GRAPH * NODES SECRET TO $roleName")
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES A,B,C TO $roleName")
    givenGraph {
      nodeGraph(sizeHint, "A") // counted
      nodeGraph(sizeHint, "A", "SECRET") // not counted, SECRET is denied
      nodeGraph(sizeHint, "B", "SECRET") // not counted, SECRET is denied
      nodeGraph(sizeHint, "C") // counted
      nodeGraph(sizeHint, "SECRET") // not counted, SECRET is denied
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(None))
      .build()

    val result = executeAs(query, runtime, username, password)

    result should beColumns("x").withSingleRow(sizeHint * 2)
  }

  val actualSize = 11

  test("relationshipCountStore count a granted relationship label") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * ELEMENTS A, B, AB TO $roleName")
    givenGraph {
      bipartiteGraph(actualSize, "A", "B", "AB")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, Seq("AB"), None)
      .build()

    val result = executeAs(query, runtime, username, password)

    result should beColumns("x").withSingleRow(actualSize * actualSize)
  }

  test("relationshipCountStore should return 0 when no relationships are granted") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * ELEMENTS A, B TO $roleName")
    givenGraph {
      bipartiteGraphMultiLabel(actualSize, "A", "B", "AB", "BA")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, Seq("AB"), None)
      .build()

    val result = executeAs(query, runtime, username, password)

    result should beColumns("x").withSingleRow(0)
  }

  test("relationshipCountStore should only count granted relationships") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * ELEMENTS A, B, AB, BA TO $roleName")
    givenGraph {
      bipartiteGraphMultiLabel(actualSize, "A", "B", "AB", "BA", "SECRET")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, Seq("AB", "BA", "SECRET"), None)
      .build()

    val result = executeAs(query, runtime, username, password)

    result should beColumns("x").withSingleRow(actualSize * actualSize * 2)
  }

  test("relationshipCountStore should return 0 when end node is not granted") {
    systemDb.executeTransactionally(s"GRANT TRAVERSE ON GRAPH * ELEMENTS A, AB TO $roleName")
    givenGraph {
      bipartiteGraph(actualSize, "A", "B", "AB")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, Seq("AB"), None)
      .build()

    val result = executeAs(query, runtime, username, password)

    result should beColumns("x").withSingleRow(0)
  }
}
