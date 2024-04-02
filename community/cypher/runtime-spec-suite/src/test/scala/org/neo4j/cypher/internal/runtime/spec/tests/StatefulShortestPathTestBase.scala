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
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionPredicate
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Selector
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RowCount
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.values.virtual.VirtualValues.pathReference

abstract class StatefulShortestPathTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private val ENABLE_LOGS = false

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    PPBFSHooks.setInstance(if (ENABLE_LOGS) LoggingPPBFSHooks.debug else PPBFSHooks.NULL)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    PPBFSHooks.setInstance(PPBFSHooks.NULL)
  }

  test("test logging is disabled in production") {
    ENABLE_LOGS shouldBe false
  }

  test("single node pattern") {

    val nodes = givenGraph {
      nodeGraph(5)
    }

    // pattern:
    // (s)
    val nfa = new TestNFABuilder(0, "s")
      .setFinalState(0)
      .build()

    val vars = Seq("s")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "s",
        "(s)",
        None,
        Set.empty,
        Set.empty,
        Set.empty,
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array[Object](n))

    runtimeResult should beColumns(vars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("single node pattern - ExpandInto") {

    val nodes = givenGraph {
      nodeGraph(5)
    }

    // pattern:
    // (s)
    val nfa = new TestNFABuilder(0, "s")
      .setFinalState(0)
      .build()

    val vars = Seq("s")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "s",
        "(s)",
        None,
        Set.empty,
        Set.empty,
        Set.empty,
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array[Object](n))

    runtimeResult should beColumns(vars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("single node juxtaposition - filter on label") {

    val x1 = givenGraph {
      val x1 = runtimeTestSupport.tx.createNode(Label.label("T")) // passes
      val x2 = runtimeTestSupport.tx.createNode(Label.label("NOT_T")) // doesn't pass
      x1
    }

    // pattern:
    // (s) (t: T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (t_inner: T)")
      .setFinalState(1)
      .build()

    val vars = Seq("s", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (t: T)",
        None,
        Set.empty,
        Set.empty,
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(Array[Object](x1, x1))

    runtimeResult should beColumns(vars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("single node juxtaposition - filter on prop") {

    val x1 = givenGraph {
      val x1 = runtimeTestSupport.tx.createNode(Label.label("T"))
      x1.setProperty("passes", true) // passes
      val x2 = runtimeTestSupport.tx.createNode(Label.label("T"))
      x2.setProperty("passes", false) // doesn't pass
      x1
    }

    // pattern:
    // (s) (t WHERE t.passes)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (t_inner WHERE t_inner.passes)")
      .setFinalState(1)
      .build()

    val vars = Seq("s", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (t WHERE t.passes)",
        None,
        Set.empty,
        Set.empty,
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(Array[Object](x1, x1))

    runtimeResult should beColumns(vars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("single node juxtaposition - filter on prop and label") {

    val x1 = givenGraph {

      // passes both label and prop
      val x1 = runtimeTestSupport.tx.createNode(Label.label("T"))
      x1.setProperty("passes", true)

      // doesn't pass prop
      val x2 = runtimeTestSupport.tx.createNode(Label.label("T"))
      x2.setProperty("passes", false)

      // doesn't pass label
      val x3 = runtimeTestSupport.tx.createNode(Label.label("NOT_T"))
      x3.setProperty("passes", true)

      // doesn't pass label and doesn't have prop
      val x4 = runtimeTestSupport.tx.createNode(Label.label("NOT_T"))

      x1
    }

    // pattern:
    // (s) (t:T WHERE t.passes)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (t_inner: T WHERE t_inner.passes)")
      .setFinalState(1)
      .build()

    val vars = Seq("s", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (t: T WHERE t.passes)",
        None,
        Set.empty,
        Set.empty,
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(Array[Object](x1, x1))

    runtimeResult should beColumns(vars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("many node juxtapositions") {

    val x1 = givenGraph {

      // passes both labels and prop
      val x1 = runtimeTestSupport.tx.createNode(Label.label("T"), Label.label("P"))
      x1.setProperty("passes", true)

      // doesn't pass prop
      val x2 = runtimeTestSupport.tx.createNode(Label.label("T"), Label.label("N"))
      x2.setProperty("passes", false)

      // doesn't pass T label
      val x3 = runtimeTestSupport.tx.createNode(Label.label("N"))
      x3.setProperty("passes", true)

      // doesn't pass N label
      val x4 = runtimeTestSupport.tx.createNode(Label.label("T"))
      x4.setProperty("passes", true)

      // doesn't pass any label and doesn't have prop
      val x5 = runtimeTestSupport.tx.createNode()

      x1
    }

    // pattern:
    // (s) (n1: P) (n2) (n3 WHERE n3.passes) (t: T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner: P)")
      .addTransition(1, 2, "(n1_inner) (n2_inner)")
      .addTransition(2, 3, "(n2_inner) (n3_inner WHERE n3_inner.passes)")
      .addTransition(3, 4, "(n3_inner) (t_inner:T)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "n2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1: P) (n2) (n3 WHERE n3.passes) (t: T)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3", "t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(Array[Object](x1, x1, x1, x1, x1))

    runtimeResult should beColumns(vars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("one hop pattern") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y, x2, x2)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected)
  }

  test("one hop pattern - ExpandInto") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y, x2, x2)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected)
  }

  test("one hop inbound") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)<-[y:R]-(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x2.createRelationshipTo(x1, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)<-[r_inner]-(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)<-[r]-(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y, x2, x2)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected)
  }

  test("one hop inbound - ExpandInto") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)<-[y:R]-(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x2.createRelationshipTo(x1, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)<-[r_inner]-(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)<-[r]-(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y, x2, x2)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected)
  }

  test("one hop undirected pattern") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]-(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]-(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]-(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y, x2, x2),
      Array[Object](x2, x2, y, x1, x1)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("one hop undirected pattern - ExpandInto") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]-(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]-(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]-(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y, x2, x2),
      Array[Object](x2, x2, y, x1, x1)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("one hop pattern - negative, not enough nodes") {

    givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      nodeGraph(1)
    }

    // pattern:
    // (s) (n1)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, not enough nodes - ExpandInto") {

    givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      nodeGraph(1)
    }

    // pattern:
    // (s) (n1)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on first inner label") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1: N)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner: N)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1: N)-[r]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on first inner label - ExpandInto") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1: N)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner: N)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1: N)-[r]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on first prop") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1 WHERE n1.passes)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner WHERE n1_inner.passes)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1 WHERE n1.passes)-[r]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on second inner label") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]->(n2: N) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner:N)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]->(n2: N) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on second inner label - ExpandInto") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]->(n2: N) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner:N)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]->(n2: N) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on second inner prop") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]->(n2 WHERE n2.passes) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner WHERE n2_inner.passes)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]->(n2 WHERE n2.passes) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on inner relationship type") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r: NOT_R]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner: NOT_R]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r: NOT_R]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on inner relationship type - ExpandInto") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r: NOT_R]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner: NOT_R]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r: NOT_R]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on inner relationship prop") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r WHERE r.passes]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner WHERE r_inner.passes]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r WHERE r.passes]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, passing on filter on inner first label but not direction of relationship") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2: N)

      val x1 = runtimeTestSupport.tx.createNode()
      val x2 = runtimeTestSupport.tx.createNode(Label.label("N"))
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1: N)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner: N)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1: N)-[r]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test(
    "one hop pattern - negative, passing on filter on inner first label but not direction of relationship - ExpandInto"
  ) {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2: N)

      val x1 = runtimeTestSupport.tx.createNode()
      val x2 = runtimeTestSupport.tx.createNode(Label.label("N"))
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1: N)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner: N)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1: N)-[r]->(n2) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, passing on filter on inner second label but not direction of relationship") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1: N)-[y:R]->(x2)

      val x1 = runtimeTestSupport.tx.createNode(Label.label("N"))
      val x2 = runtimeTestSupport.tx.createNode()
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]->(n2: N) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner: N)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]->(n2: N) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - negative, filtered on nonInlineablePreFilter") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      x1.setProperty("prop", 1)
      x2.setProperty("prop", 2)
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r]->(n2) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r]->(n2) (t)",
        Some("n1.prop > n2.prop"),
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("one hop pattern - positive, filtered on everything") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1: N1 {passes: true})-[y:R {passes: true}]->(x2:N2 {passes: true})

      val x1 = runtimeTestSupport.tx.createNode(Label.label("N1"))
      val x2 = runtimeTestSupport.tx.createNode(Label.label("N2"))
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))

      x1.setProperty("passes", true)
      x2.setProperty("passes", true)
      y.setProperty("passes", true)
      x1.setProperty("prop", 1)
      x2.setProperty("prop", 2)
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1: N1 WHERE n1.passes)-[r: R WHERE r.passes]->(n2: N2 WHERE n2.passes) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner: N1 WHERE n1_inner.passes)")
      .addTransition(1, 2, "(n1_inner)-[r_inner: R WHERE r_inner.passes]->(n2_inner: N2 WHERE n2_inner.passes)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1: N1 WHERE n1.passes)-[r: R WHERE r.passes]->(n2: N2 WHERE n2.passes) (t)",
        Some("n1.prop < n2.prop"),
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y, x2, x2)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected)
  }

  test("one hop pattern - positive, filtered on everything - ExpandInto") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1: N1 {passes: true})-[y:R {passes: true}]->(x2:N2 {passes: true})

      val x1 = runtimeTestSupport.tx.createNode(Label.label("N1"))
      val x2 = runtimeTestSupport.tx.createNode(Label.label("N2"))
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))

      x1.setProperty("passes", true)
      x2.setProperty("passes", true)
      y.setProperty("passes", true)
      x1.setProperty("prop", 1)
      x2.setProperty("prop", 2)
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1: N1 WHERE n1.passes)-[r: R WHERE r.passes]->(n2: N2 WHERE n2.passes) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner: N1 WHERE n1_inner.passes)")
      .addTransition(1, 2, "(n1_inner)-[r_inner: R WHERE r_inner.passes]->(n2_inner: N2 WHERE n2_inner.passes)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1: N1 WHERE n1.passes)-[r: R WHERE r.passes]->(n2: N2 WHERE n2.passes) (t)",
        Some("n1.prop < n2.prop"),
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "t_inner" -> "t"),
        Set("r_inner" -> "r"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.filter("t.passes")
      .|.nodeByLabelScan("t", "N2")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y, x2, x2)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected)
  }

  test("two hop pattern") {

    val (x1, y1, x2, y2, x3) = givenGraph {
      // GRAPH:
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)

      val Seq(x1, x2, x3) = nodeGraph(3)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3)
    }

    // pattern:
    // (s) (n1)-[r1]->(n2)-[r2]->(n3) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner)-[r2_inner]->(n3_inner)")
      .addTransition(3, 4, "(n3_inner) (t_inner)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r1]->(n2)-[r2]->(n3) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3", "t_inner" -> "t"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y1, x2, y2, x3, x3)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected)
  }

  test("two hop pattern - ExpandInto") {

    val (x1, y1, x2, y2, x3) = givenGraph {
      // GRAPH:
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)

      val Seq(x1, x2, x3) = nodeGraph(3)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3)
    }

    // pattern:
    // (s) (n1)-[r1]->(n2)-[r2]->(n3) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner)-[r2_inner]->(n3_inner)")
      .addTransition(3, 4, "(n3_inner) (t)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r1]->(n2)-[r2]->(n3) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](x1, x1, y1, x2, y2, x3, x3)
    )

    runtimeResult should beColumns(vars: _*).withRows(expected)
  }

  test("two hop pattern - negative, duplicate relationships, loop graph") {

    val (x, y) = givenGraph {
      // GRAPH:
      // (x)-[y:R]->(x)

      val Seq(x) = nodeGraph(1)
      val y = x.createRelationshipTo(x, RelationshipType.withName("R"))
      (x, y)
    }

    // pattern:
    // (s) (n1)-[r1]->(n2)-[r2]->(n3) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner)-[r2_inner]->(n3_inner)")
      .addTransition(3, 4, "(n3_inner) (t_inner)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r1]->(n2)-[r2]->(n3) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3", "t_inner" -> "t"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("two hop pattern - negative, duplicate relationships, loop graph - ExpandInto") {

    val (x, y) = givenGraph {
      // GRAPH:
      // (x)-[y:R]->(x)

      val Seq(x) = nodeGraph(1)
      val y = x.createRelationshipTo(x, RelationshipType.withName("R"))
      (x, y)
    }

    // pattern:
    // (s) (n1)-[r1]->(n2)-[r2]->(n3) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner)-[r2_inner]->(n3_inner)")
      .addTransition(3, 4, "(n3_inner) (t)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r1]->(n2)-[r2]->(n3) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("two hop pattern - negative, duplicate relationships, one hop graph") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r1]->(n2)<-[r2]-(n3) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner)<-[r2_inner]-(n3_inner)")
      .addTransition(3, 4, "(n3_inner) (t_inner)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r1]->(n2)<-[r2]-(n3) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3", "t_inner" -> "t"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("two hop pattern - negative, duplicate relationships, one hop graph - ExpandInto") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r1]->(n2)<-[r2]-(n3) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner)<-[r2_inner]-(n3_inner)")
      .addTransition(3, 4, "(n3_inner) (t)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r1]->(n2)<-[r2]-(n3) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("two hop undirected pattern - negative, duplicate relationships, one hop graph") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r1]-(n2)-[r2]-(n3) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]-(n2_inner)")
      .addTransition(2, 3, "(n2_inner)-[r2_inner]-(n3_inner)")
      .addTransition(3, 4, "(n3_inner) (t_inner)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r1]-(n2)-[r2]-(n3) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3", "t_inner" -> "t"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("cached node properties on NFA predicate expression variables") {
    val (n1, n2) = givenGraph {
      val graph = fromTemplate("""
        (:S)-->(n1)-->(n2:T)
      """)

      (graph node "n1", graph node "n2")
    }
    n1.setProperty("prop", 1)
    n2.setProperty("prop", 2)

    // the cacheNFromStore should not persist
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner WHERE cacheNFromStore[n2_inner.prop] = 1)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("t")
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))+ (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("s", "S")
      .nodeByLabelScan("t", "T")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("t").withNoRows()
  }

  test("cached relationship properties on NFA predicate expression variables") {
    val (r1, r2) = givenGraph {
      val graph = fromTemplate("""
        (:S)-[r1]->()-[r2]->(:T)
      """)

      (graph rel "r1", graph rel "r2")
    }
    r1.setProperty("prop", 1)
    r2.setProperty("prop", 2)

    // the cacheNFromStore should not persist
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner WHERE cacheRFromStore[r_inner.prop] = 1]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("t")
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))+ (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("s", "S")
      .nodeByLabelScan("t", "T")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("t").withNoRows()
  }

  test("two hop undirected pattern - negative, duplicate relationships, one hop graph - ExpandInto") {

    val (x1, y, x2) = givenGraph {
      // GRAPH:
      // (x1)-[y:R]->(x2)

      val Seq(x1, x2) = nodeGraph(2)
      val y = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      (x1, y, x2)
    }

    // pattern:
    // (s) (n1)-[r1]-(n2)-[r2]-(n3) (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]-(n2_inner)")
      .addTransition(2, 3, "(n2_inner)-[r2_inner]-(n3_inner)")
      .addTransition(3, 4, "(n3_inner) (t)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) (n1)-[r1]-(n2)-[r2]-(n3) (t)",
        None,
        Set.empty,
        Set.empty,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns(vars: _*).withNoRows()
  }

  test("* quantified path pattern") {

    val (x1, y1, x2, y2, x3) = givenGraph {
      // GRAPH:
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)

      val Seq(x1, x2, x3) = nodeGraph(3)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3)
    }

    // pattern:
    // (s) ((n1)-[r]->(n2))* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 3, "(s) (t_inner)")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = vars :+ "p"

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        x1,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x1,
        pathReference(Array(x1.getId), Array[Long]())
      ),

      //  this:   //\s+Seq
      Array[Object](
        x2,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x2,
        pathReference(Array(x2.getId), Array[Long]())
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x3,
        pathReference(Array(x3.getId), Array[Long]())
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1),
        TrailTestBase.listOf(y1),
        TrailTestBase.listOf(x2),
        x2,
        pathReference(Array(x1.getId, x2.getId), Array(y1.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2),
        TrailTestBase.listOf(y2),
        TrailTestBase.listOf(x3),
        x3,
        pathReference(Array(x2.getId, x3.getId), Array(y2.getId))
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2),
        TrailTestBase.listOf(y1, y2),
        TrailTestBase.listOf(x2, x3),
        x3,
        pathReference(Array(x1.getId, x2.getId, x3.getId), Array(y1.getId, y2.getId))
      )
    )

    runtimeResult should beColumns(retVars: _*).withRows(oneToOneSortedPaths("p", expected))
  }

  test("* quantified path pattern - ExpandInto") {

    val (x1, y1, x2, y2, x3) = givenGraph {
      // GRAPH:
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)

      val Seq(x1, x2, x3) = nodeGraph(3)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3)
    }

    // pattern:
    // (s) ((n1)-[r]->(n2))* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 3, "(s) (t)")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = vars :+ "p"

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set.empty,
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        x1,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x1,
        pathReference(Array(x1.getId), Array[Long]())
      ),

      //  this:   //\s+Seq
      Array[Object](
        x2,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x2,
        pathReference(Array(x2.getId), Array[Long]())
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x3,
        pathReference(Array(x3.getId), Array[Long]())
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1),
        TrailTestBase.listOf(y1),
        TrailTestBase.listOf(x2),
        x2,
        pathReference(Array(x1.getId, x2.getId), Array(y1.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2),
        TrailTestBase.listOf(y2),
        TrailTestBase.listOf(x3),
        x3,
        pathReference(Array(x2.getId, x3.getId), Array(y2.getId))
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2),
        TrailTestBase.listOf(y1, y2),
        TrailTestBase.listOf(x2, x3),
        x3,
        pathReference(Array(x1.getId, x2.getId, x3.getId), Array(y1.getId, y2.getId))
      )
    )

    runtimeResult should beColumns(retVars: _*).withRows(oneToOneSortedPaths("p", expected))
  }

  test("+ quantified path pattern") {

    val (x1, y1, x2, y2, x3) = givenGraph {
      // GRAPH:
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)

      val Seq(x1, x2, x3) = nodeGraph(3)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3)
    }

    // pattern:
    // (s) ((n1)-[r]->(n2))+ (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = vars :+ "p"

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))+ (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        x1,
        TrailTestBase.listOf(x1),
        TrailTestBase.listOf(y1),
        TrailTestBase.listOf(x2),
        x2,
        pathReference(Array(x1.getId, x2.getId), Array(y1.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2),
        TrailTestBase.listOf(y2),
        TrailTestBase.listOf(x3),
        x3,
        pathReference(Array(x2.getId, x3.getId), Array(y2.getId))
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2),
        TrailTestBase.listOf(y1, y2),
        TrailTestBase.listOf(x2, x3),
        x3,
        pathReference(Array(x1.getId, x2.getId, x3.getId), Array(y1.getId, y2.getId))
      )
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("+ quantified path pattern - ExpandInto") {

    val (x1, y1, x2, y2, x3) = givenGraph {
      // GRAPH:
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)

      val Seq(x1, x2, x3) = nodeGraph(3)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3)
    }

    // pattern:
    // (s) ((n1)-[r]->(n2))+ (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = vars :+ "p"

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))+ (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set.empty,
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        x1,
        TrailTestBase.listOf(x1),
        TrailTestBase.listOf(y1),
        TrailTestBase.listOf(x2),
        x2,
        pathReference(Array(x1.getId, x2.getId), Array(y1.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2),
        TrailTestBase.listOf(y2),
        TrailTestBase.listOf(x3),
        x3,
        pathReference(Array(x2.getId, x3.getId), Array(y2.getId))
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2),
        TrailTestBase.listOf(y1, y2),
        TrailTestBase.listOf(x2, x3),
        x3,
        pathReference(Array(x1.getId, x2.getId, x3.getId), Array(y1.getId, y2.getId))
      )
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected, listInAnyOrder = true)
  }

  test("expression variable for VariablePredicate on target node shouldn't interfere with successive operators") {

    val (n3, n5) = givenGraph {
      // We return a path to n3, then a path to n5, then to n3 again. This will cause a bug if
      // the expression variable for VariablePredicate on target node interferes with the projection.

      val graph = fromTemplate(
        """    .-----------------.
          |    |                 v
          |(n0:S)->(n1)->(n2)->(n3:T)
          |    |
          |    '-->(n4)-->(n5:T)
          |
          |""".stripMargin
      )

      (graph node "n3", graph node "n5")
    }

    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner:T)")
      .setFinalState(3)
      .build()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("u")
      .projection("t as u")
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))+ (t: T)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array(n3),
      Array(n5),
      Array(n3)
    )

    runtimeResult should beColumns("u").withRows(expected, listInAnyOrder = true)
  }

  test("undirected * quantified path pattern") {

    val (x1, y1, x2, y2, x3) = givenGraph {
      // GRAPH:
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)

      val Seq(x1, x2, x3) = nodeGraph(3)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3)
    }

    // pattern:
    // (s) ((n1)-[r]-(n2))* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 3, "(s) (t_inner)")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]-(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = vars :+ "p"

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]-(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        x1,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x1,
        pathReference(Array(x1.getId), Array[Long]())
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x2,
        pathReference(Array(x2.getId), Array[Long]())
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x3,
        pathReference(Array(x3.getId), Array[Long]())
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1),
        TrailTestBase.listOf(y1),
        TrailTestBase.listOf(x2),
        x2,
        pathReference(Array(x1.getId, x2.getId), Array(y1.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2),
        TrailTestBase.listOf(y1),
        TrailTestBase.listOf(x1),
        x1,
        pathReference(Array(x2.getId, x1.getId), Array(y1.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2),
        TrailTestBase.listOf(y2),
        TrailTestBase.listOf(x3),
        x3,
        pathReference(Array(x2.getId, x3.getId), Array(y2.getId))
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(x3),
        TrailTestBase.listOf(y2),
        TrailTestBase.listOf(x2),
        x2,
        pathReference(Array(x3.getId, x2.getId), Array(y2.getId))
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2),
        TrailTestBase.listOf(y1, y2),
        TrailTestBase.listOf(x2, x3),
        x3,
        pathReference(Array(x1.getId, x2.getId, x3.getId), Array(y1.getId, y2.getId))
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(x3, x2),
        TrailTestBase.listOf(y2, y1),
        TrailTestBase.listOf(x2, x1),
        x1,
        pathReference(Array(x3.getId, x2.getId, x1.getId), Array(y2.getId, y1.getId))
      )
    )

    runtimeResult should beColumns(retVars: _*).withRows(oneToOneSortedPaths("p", expected))
  }

  test("undirected * quantified path pattern - ExpandInto") {

    val (x1, y1, x2, y2, x3) = givenGraph {
      // GRAPH:
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)

      val Seq(x1, x2, x3) = nodeGraph(3)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3)
    }

    // pattern:
    // (s) ((n1)-[r]-(n2))* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 3, "(s) (T)")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]-(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (T)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = vars :+ "p"

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]-(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set.empty,
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        x1,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x1,
        pathReference(Array(x1.getId), Array[Long]())
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x2,
        pathReference(Array(x2.getId), Array[Long]())
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        TrailTestBase.listOf(),
        x3,
        pathReference(Array(x3.getId), Array[Long]())
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1),
        TrailTestBase.listOf(y1),
        TrailTestBase.listOf(x2),
        x2,
        pathReference(Array(x1.getId, x2.getId), Array(y1.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2),
        TrailTestBase.listOf(y1),
        TrailTestBase.listOf(x1),
        x1,
        pathReference(Array(x2.getId, x1.getId), Array(y1.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2),
        TrailTestBase.listOf(y2),
        TrailTestBase.listOf(x3),
        x3,
        pathReference(Array(x2.getId, x3.getId), Array(y2.getId))
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(x3),
        TrailTestBase.listOf(y2),
        TrailTestBase.listOf(x2),
        x2,
        pathReference(Array(x3.getId, x2.getId), Array(y2.getId))
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2),
        TrailTestBase.listOf(y1, y2),
        TrailTestBase.listOf(x2, x3),
        x3,
        pathReference(Array(x1.getId, x2.getId, x3.getId), Array(y1.getId, y2.getId))
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(x3, x2),
        TrailTestBase.listOf(y2, y1),
        TrailTestBase.listOf(x2, x1),
        x1,
        pathReference(Array(x3.getId, x2.getId, x1.getId), Array(y2.getId, y1.getId))
      )
    )

    runtimeResult should beColumns(retVars: _*).withRows(oneToOneSortedPaths("p", expected))
  }

  test("{2, 3} quantified path pattern") {
    val (x1, y1, x2, y2, x3, y3, x4, y4, x5) = givenGraph {
      // GRAPH: (4 hops, one longer than pattern)
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)-[y3:R]->(x4)-[y4:R]->(x5)

      val Seq(x1, x2, x3, x4, x5) = nodeGraph(5)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      val y3 = x3.createRelationshipTo(x4, RelationshipType.withName("R"))
      val y4 = x4.createRelationshipTo(x5, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3, y3, x4, y4, x5)
    }

    // pattern:
    // (s) ((n1)-[r]->(n2)){2, 3} (t)
    val nfa = new TestNFABuilder(0, "s")
      // 0 hops
      .addTransition(0, 1, "(s) (n1_1_inner)")
      .addTransition(1, 2, "(n1_1_inner)-[r_1_inner]->(n2_1_inner)")
      // 1 hop
      .addTransition(2, 3, "(n2_1_inner) (n1_2_inner)")
      .addTransition(3, 4, "(n1_2_inner)-[r_2_inner]->(n2_2_inner)")
      .addTransition(4, 7, "(n2_2_inner) (t_inner)")
      // 2 hops
      .addTransition(4, 5, "(n2_2_inner) (n1_3_inner)")
      .addTransition(5, 6, "(n1_3_inner)-[r_3_inner]->(n2_3_inner)")
      .addTransition(6, 7, "(n2_3_inner) (t_inner)")
      // 3 hops
      .setFinalState(7)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = vars :+ "p"

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2)){2, 3} (t)",
        None,
        Set(
          "n1_1_inner" -> "n1",
          "n1_2_inner" -> "n1",
          "n1_3_inner" -> "n1",
          "n2_1_inner" -> "n2",
          "n2_2_inner" -> "n2",
          "n2_3_inner" -> "n2"
        ),
        Set(
          "r_1_inner" -> "r",
          "r_2_inner" -> "r",
          "r_3_inner" -> "r"
        ),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2),
        TrailTestBase.listOf(y1, y2),
        TrailTestBase.listOf(x2, x3),
        x3,
        pathReference(Array(x1.getId, x2.getId, x3.getId), Array(y1.getId, y2.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2, x3),
        TrailTestBase.listOf(y2, y3),
        TrailTestBase.listOf(x3, x4),
        x4,
        pathReference(Array(x2.getId, x3.getId, x4.getId), Array(y2.getId, y3.getId))
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(x3, x4),
        TrailTestBase.listOf(y3, y4),
        TrailTestBase.listOf(x4, x5),
        x5,
        pathReference(Array(x3.getId, x4.getId, x5.getId), Array(y3.getId, y4.getId))
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2, x3),
        TrailTestBase.listOf(y1, y2, y3),
        TrailTestBase.listOf(x2, x3, x4),
        x4,
        pathReference(Array(x1.getId, x2.getId, x3.getId, x4.getId), Array(y1.getId, y2.getId, y3.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2, x3, x4),
        TrailTestBase.listOf(y2, y3, y4),
        TrailTestBase.listOf(x3, x4, x5),
        x5,
        pathReference(Array(x2.getId, x3.getId, x4.getId, x5.getId), Array(y2.getId, y3.getId, y4.getId))
      )
    )

    runtimeResult should beColumns(retVars: _*).withRows(oneToOneSortedPaths("p", expected))
  }

  test("{2, 3} quantified path pattern - ExpandInto") {
    val (x1, y1, x2, y2, x3, y3, x4, y4, x5) = givenGraph {
      // GRAPH: (4 hops, one longer than pattern)
      // (x1)-[y1:R]->(x2)-[y2:R]->(x3)-[y3:R]->(x4)-[y4:R]->(x5)

      val Seq(x1, x2, x3, x4, x5) = nodeGraph(5)
      val y1 = x1.createRelationshipTo(x2, RelationshipType.withName("R"))
      val y2 = x2.createRelationshipTo(x3, RelationshipType.withName("R"))
      val y3 = x3.createRelationshipTo(x4, RelationshipType.withName("R"))
      val y4 = x4.createRelationshipTo(x5, RelationshipType.withName("R"))
      (x1, y1, x2, y2, x3, y3, x4, y4, x5)
    }

    // pattern:
    // (s) ((n1)-[r]->(n2)){2, 3} (t)
    val nfa = new TestNFABuilder(0, "s")
      // 0 hops
      .addTransition(0, 1, "(s) (n1_1_inner)")
      .addTransition(1, 2, "(n1_1_inner)-[r_1_inner]->(n2_1_inner)")
      // 1 hop
      .addTransition(2, 3, "(n2_1_inner) (n1_2_inner)")
      .addTransition(3, 4, "(n1_2_inner)-[r_2_inner]->(n2_2_inner)")
      .addTransition(4, 7, "(n2_2_inner) (t)")
      // 2 hops
      .addTransition(4, 5, "(n2_2_inner) (n1_3_inner)")
      .addTransition(5, 6, "(n1_3_inner)-[r_3_inner]->(n2_3_inner)")
      .addTransition(6, 7, "(n2_3_inner) (t)")
      // 3 hops
      .setFinalState(7)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = vars :+ "p"

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2)){2, 3} (t)",
        None,
        Set(
          "n1_1_inner" -> "n1",
          "n1_2_inner" -> "n1",
          "n1_3_inner" -> "n1",
          "n2_1_inner" -> "n2",
          "n2_2_inner" -> "n2",
          "n2_3_inner" -> "n2"
        ),
        Set(
          "r_1_inner" -> "r",
          "r_2_inner" -> "r",
          "r_3_inner" -> "r"
        ),
        Set.empty,
        Set.empty,
        Selector.Shortest(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2),
        TrailTestBase.listOf(y1, y2),
        TrailTestBase.listOf(x2, x3),
        x3,
        pathReference(Array(x1.getId, x2.getId, x3.getId), Array(y1.getId, y2.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2, x3),
        TrailTestBase.listOf(y2, y3),
        TrailTestBase.listOf(x3, x4),
        x4,
        pathReference(Array(x2.getId, x3.getId, x4.getId), Array(y2.getId, y3.getId))
      ),
      Array[Object](
        x3,
        TrailTestBase.listOf(x3, x4),
        TrailTestBase.listOf(y3, y4),
        TrailTestBase.listOf(x4, x5),
        x5,
        pathReference(Array(x3.getId, x4.getId, x5.getId), Array(y3.getId, y4.getId))
      ),
      Array[Object](
        x1,
        TrailTestBase.listOf(x1, x2, x3),
        TrailTestBase.listOf(y1, y2, y3),
        TrailTestBase.listOf(x2, x3, x4),
        x4,
        pathReference(Array(x1.getId, x2.getId, x3.getId, x4.getId), Array(y1.getId, y2.getId, y3.getId))
      ),
      Array[Object](
        x2,
        TrailTestBase.listOf(x2, x3, x4),
        TrailTestBase.listOf(y2, y3, y4),
        TrailTestBase.listOf(x3, x4, x5),
        x5,
        pathReference(Array(x2.getId, x3.getId, x4.getId, x5.getId), Array(y2.getId, y3.getId, y4.getId))
      )
    )

    runtimeResult should beColumns(retVars: _*).withRows(oneToOneSortedPaths("p", expected))
  }

  test("nested plan expressions should work inside an NFA transition predicate") {

    val v = givenGraph {
      fromTemplate("""
             .---->()-->()-->()
             |                |
             |                v
        (u:User)-->()-->()-->(v)-->(:N)
             |                |
             |                v
             '---->()----->(w:User)
      """) node "v"
    }

    // initialId = 100 so that it doesn't clash with the main query when setting attributes
    val npe = new LogicalPlanBuilder(wholePlan = false, resolver = this, initialId = 100)
      .filter("n_npe:N")
      .expand("(v_inner)-[r_npe]->(n_npe)")
      .argument("v_inner")
      .build()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("v")
      .statefulShortestPath(
        "u",
        "w",
        "SHORTEST 1 ((u:User) ((a)-[r]->(b))+ (v)--(w:User) WHERE (v)-->(:N))",
        None,
        Set(("a_inner", "a")),
        Set(("r_inner", "r")),
        Set(("v_inner", "v")),
        Set(("  UNNAMED22", "  UNNAMED0")),
        Selector.Shortest(1),
        new TestNFABuilder(0, "u")
          .addTransition(0, 1, "(u) (a_inner)")
          .addTransition(1, 2, "(a_inner)-[r_inner]->(b_inner)")
          .addTransition(2, 1, "(b_inner) (a_inner)")
          .addTransition(
            2 -> "b_inner",
            3 -> "v_inner",
            NodeJuxtapositionPredicate(Some(VariablePredicate(
              varFor("v_inner"),
              NestedPlanExistsExpression(npe, "EXISTS { (v_inner)-->(:N) }")(pos)
            )))
          )
          .addTransition(3, 4, "(v_inner)-[`  UNNAMED22`]-(w)")
          .setFinalState(4)
          .build(),
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("u", "User", IndexOrderNone)
      .nodeByLabelScan("w", "User", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("v").withRows(Seq(Array(v)))
  }

  test("should find non-shortest paths when all shortest paths have duplicate relationships") {

    // For this test we want PPBFS to propagate, and exclude the shortest paths in the product graph due to duplicate
    // relationship when projected back to the data graph. So,
    //
    //  * the shortest path in the product graph should include the same physical relationship twice,
    //
    //  * It should end in a target node which has a longer path to it which remains a trail after projection to the
    //  data graph,
    //
    //  * and this longer path should share some part of it's tail with the shorter path so that we propagation will
    //  occur.
    //
    // Consider the following graph and pattern,
    //
    //  Graph:
    //
    //    (x0: S & T)-[y0:R1]->(x1)
    //       |                 ^
    //    [y1:R2]             /
    //       |            [y3:R2]
    //       V              /
    //      (x2)-[y2:R1]->(x3)
    //
    //  Pattern:
    //                                0      1           2            3      4
    //    MATCH SHORTEST GROUPS 9999 (s: S) ((n1)-[r1]->(n2)-[r2:R1]-(n3))+ (t: T)
    //
    // The shortest path which matches is given by:
    //
    //   (x0)-[y0]->(x1)<-[y0]-(x0),
    //
    // but this path has a duplicate rel y1. So the shortest trail which matches the pattern is:
    //
    //  (x0)-[y1]->(x2)-[y2]->(x3)-[y3]->(x1)<-[y0]-(x0),
    //
    // and returning this trail requires us to propagate the non-shortest path data leading up to (x1) through <-[y0]-

    val (x0, y0, x1, y1, x2, y2, x3, y3) = givenGraph {

      val Seq(x0, x1, x2, x3) = nodeGraph(4)
      x0.addLabel(Label.label("S"))
      x0.addLabel(Label.label("T"))

      val y1 = x0.createRelationshipTo(x1, RelationshipType.withName("R1"))
      val y2 = x0.createRelationshipTo(x2, RelationshipType.withName("R2"))
      val y3 = x2.createRelationshipTo(x3, RelationshipType.withName("R1"))
      val y4 = x3.createRelationshipTo(x1, RelationshipType.withName("R2"))

      (x0, y1, x1, y2, x2, y3, x3, y4)
    }

    // pattern:
    // (s) ((n1)-[r1]->(n2)-[r2:R1]-(n3))+ (t: T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner)-[r2_inner: R1]-(n3_inner)")
      .addTransition(3, 1, "(n3_inner) (n1_inner)")
      .addTransition(3, 4, "(n3_inner) (t_inner: T)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r1]->(n2)-[r2:R1]-(n3))+ (t: T)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.ShortestGroups(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Seq(
        Array[Object](
          x0,
          TrailTestBase.listOf(x0, x3),
          TrailTestBase.listOf(y1, y3),
          TrailTestBase.listOf(x2, x1),
          TrailTestBase.listOf(y2, y0),
          TrailTestBase.listOf(x3, x0),
          x0
        )
      )
    )

    runtimeResult should beColumns(vars: _*).withRows(inPartialOrder(expected))
  }

  test("should find non-shortest paths when all shortest paths have duplicate relationships - ExpandInto") {

    // For this test we want PPBFS to propagate, and exclude the shortest paths in the product graph due to duplicate
    // relationship when projected back to the data graph. So,
    //
    //  * the shortest path in the product graph should include the same physical relationship twice,
    //
    //  * It should end in a target node which has a longer path to it which remains a trail after projection to the
    //  data graph,
    //
    //  * and this longer path should share some part of it's tail with the shorter path so that we propagation will
    //  occur.
    //
    // Consider the following graph and pattern,
    //
    //  Graph:
    //
    //    (x0: S & T)-[y0:R1]->(x1)
    //       |                 ^
    //    [y1:R2]             /
    //       |            [y3:R2]
    //       V              /
    //      (x2)-[y2:R1]->(x3)
    //
    //  Pattern:
    //                                0      1           2            3      4
    //    MATCH SHORTEST GROUPS 9999 (s: S) ((n1)-[r1]->(n2)-[r2:R1]-(n3))+ (t: T)
    //
    // The shortest path which matches is given by:
    //
    //   (x0)-[y0]->(x1)<-[y0]-(x0),
    //
    // but this path has a duplicate rel y1. So the shortest trail which matches the pattern is:
    //
    //  (x0)-[y1]->(x2)-[y2]->(x3)-[y3]->(x1)<-[y0]-(x0),
    //
    // and returning this trail requires us to propagate the non-shortest path data leading up to (x1) through <-[y0]-

    val (x0, y0, x1, y1, x2, y2, x3, y3) = givenGraph {

      val Seq(x0, x1, x2, x3) = nodeGraph(4)
      x0.addLabel(Label.label("S"))
      x0.addLabel(Label.label("T"))

      val y1 = x0.createRelationshipTo(x1, RelationshipType.withName("R1"))
      val y2 = x0.createRelationshipTo(x2, RelationshipType.withName("R2"))
      val y3 = x2.createRelationshipTo(x3, RelationshipType.withName("R1"))
      val y4 = x3.createRelationshipTo(x1, RelationshipType.withName("R2"))

      (x0, y1, x1, y2, x2, y3, x3, y4)
    }

    // pattern:
    // (s) ((n1)-[r1]->(n2)-[r2:R1]-(n3))+ (t: T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner]->(n2_inner)")
      .addTransition(2, 3, "(n2_inner)-[r2_inner: R1]-(n3_inner)")
      .addTransition(3, 1, "(n3_inner) (n1_inner)")
      .addTransition(3, 4, "(n3_inner) (t)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "n3", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r1]->(n2)-[r2:R1]-(n3))+ (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2", "n3_inner" -> "n3"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Set.empty,
        Set.empty,
        Selector.ShortestGroups(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("t", "T")
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Seq(
        Array[Object](
          x0,
          TrailTestBase.listOf(x0, x3),
          TrailTestBase.listOf(y1, y3),
          TrailTestBase.listOf(x2, x1),
          TrailTestBase.listOf(y2, y0),
          TrailTestBase.listOf(x3, x0),
          x0
        )
      )
    )

    runtimeResult should beColumns(vars: _*).withRows(inPartialOrder(expected))
  }

  test("grid graph - start anywhere, interleaved RIGHT DOWN pattern, end with bottom right") {
    val dim = 5
    val bottomRightLabel = s"`${dim - 1},${dim - 1}`"

    val diagonal = givenGraph {
      val (nodes, _) = gridGraph(dim, dim)

      // Extract nodes on the diagonal, as these will be the only nodes we return the pattern
      val diagonal = (0 until dim).map(_ * (dim + 1)).map(nodes)
      diagonal
    }

    // pattern:
    // (s) ((n1)-[r1:RIGHT]->()-[r2:DOWN]-(n2))+ (t: $bottomRightLabel)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner: RIGHT]->(anon_inner)")
      .addTransition(2, 3, "(anon_inner)-[r2_inner: DOWN]->(n2_inner)")
      .addTransition(3, 1, "(n2_inner) (n1_inner)")
      .addTransition(3, 4, s"(n2_inner) (t_inner: $bottomRightLabel)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "t")
    val retVars = Seq("s", "n1", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        s"(s) ((n1)-[r1:RIGHT]->()-[r2:DOWN]-(n2))+ (t: $bottomRightLabel)",
        None,
        Set("n1_inner" -> "n1", "anon_inner" -> "anon", "n2_inner" -> "n2"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.ShortestGroups(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    def rowStartingWithNodeAtIndexInDiagonal(i: Int): Array[Object] = {
      val n1 = (i until diagonal.size - 1).map(diagonal)
      val n2 = (i + 1 to diagonal.size - 1).map(diagonal)
      Array[Object](diagonal(i), TrailTestBase.listOf(n1: _*), TrailTestBase.listOf(n2: _*), diagonal.last)
    }
    val expected = (0 until diagonal.size - 1).map(rowStartingWithNodeAtIndexInDiagonal)

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("grid graph - start anywhere, interleaved RIGHT DOWN pattern, end with bottom right - ExpandInto") {
    val dim = 5
    val bottomRightLabel = s"${dim - 1},${dim - 1}"

    val diagonal = givenGraph {
      val (nodes, _) = gridGraph(dim, dim)

      // Extract nodes on the diagonal, as these will be the only nodes we return the pattern
      val diagonal = (0 until dim).map(_ * (dim + 1)).map(nodes)
      diagonal
    }

    // pattern:
    // (s) ((n1)-[r1:RIGHT]->()-[r2:DOWN]-(n2))+ (t: $bottomRightLabel)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner: RIGHT]->(anon_inner)")
      .addTransition(2, 3, "(anon_inner)-[r2_inner: DOWN]->(n2_inner)")
      .addTransition(3, 1, "(n2_inner) (n1_inner)")
      .addTransition(3, 4, s"(n2_inner) (t)")
      .setFinalState(4)
      .build()

    val vars = Seq("s", "n1", "r1", "n2", "r2", "t")
    val retVars = Seq("s", "n1", "n2", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        s"(s) ((n1)-[r1:RIGHT]->()-[r2:DOWN]-(n2))+ (t)",
        None,
        Set("n1_inner" -> "n1", "anon_inner" -> "anon", "n2_inner" -> "n2"),
        Set("r1_inner" -> "r1", "r2_inner" -> "r2"),
        Set.empty,
        Set.empty,
        Selector.ShortestGroups(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("t", bottomRightLabel)
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    def rowStartingWithNodeAtIndexInDiagonal(i: Int): Array[Object] = {
      val n1 = (i until diagonal.size - 1).map(diagonal)
      val n2 = (i + 1 to diagonal.size - 1).map(diagonal)
      Array[Object](diagonal(i), TrailTestBase.listOf(n1: _*), TrailTestBase.listOf(n2: _*), diagonal.last)
    }
    val expected = (0 until diagonal.size - 1).map(rowStartingWithNodeAtIndexInDiagonal)

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("grid graph - top left to bottom right, where we need to go traverse entire rows before we go one step down") {

    // This test tests propagation
    val dim = 5 // Don't change
    val topLeftLabel = s"0,0"
    val bottomRightLabel = s"`${dim - 1},${dim - 1}`"

    val nodes = givenGraph {
      val (nodes, _) = gridGraph(dim, dim)
      nodes
    }

    // pattern:
    //
    // (s: $topLeftLabel) ( ((row)-[r1:RIGHT]-()){5} (rowEnd)-[r2:DOWN]->()? )+ (t: $bottomRightLabel)
    //
    // NOTE - The nfa is structured as if the inner quantification was explicitly expanded, but that is tedious
    // to write, so we simplify it's description with {5}. Even if nested group variables (or really, quantification's)
    // aren't allowed in cypher, we can collect all row vars into one group here if we want.

    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (row_1_inner)")
      .addTransition(1, 2, "(row_1_inner)-[r1_1_inner:RIGHT]-(anon1_1_inner)")
      // 1 hop traversed
      .addTransition(2, 3, "(anon1_1_inner) (row_2_inner)")
      .addTransition(3, 4, "(row_2_inner)-[r1_2_inner:RIGHT]-(anon1_2_inner)")
      // 2 hops traversed
      .addTransition(4, 5, "(anon1_2_inner) (row_3_inner)")
      .addTransition(5, 6, "(row_3_inner)-[r1_3_inner:RIGHT]-(anon1_3_inner)")
      // 3 hops traversed
      .addTransition(6, 7, "(anon1_3_inner) (row_4_inner)")
      .addTransition(7, 8, "(row_4_inner)-[r1_4_inner:RIGHT]-(anon1_4_inner)")
      .addTransition(8, 11, s"(anon1_4_inner) (t_inner: $bottomRightLabel)") // skip second inner pattern with "?"
      // 5 hops traversed
      .addTransition(8, 9, s"(anon1_4_inner) (rowEnd_inner)")
      .addTransition(9, 10, "(rowEnd_inner)-[r2_inner:DOWN]->(anon2_inner)")
      // - Full outer quantification traversed
      .addTransition(10, 1, "(anon2_inner) (row_1_inner)")
      .addTransition(10, 11, s"(anon2_inner) (t_inner: $bottomRightLabel)")
      .setFinalState(11)
      .build()

    val retVars = Seq("s", "row", "rowEnd", "t")
    val vars = Seq("s", "row", "r2", "anon2", "rowEnd", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        s"(s) ( ((row)-[r1:RIGHT]-()){5} (rowEnd)-[r2:DOWN]->() )+ (t: $bottomRightLabel)",
        None,
        Set(
          "row_1_inner" -> "row",
          "row_2_inner" -> "row",
          "row_3_inner" -> "row",
          "row_4_inner" -> "row",
          "rowEnd_inner" -> "rowEnd",
          // TODO: Anonymous grouping variables are currently being added by the planner, see https://trello.com/c/fsucxJdb/
          //       Collecting and projecting them is a waste of resources.
          "anon1_1_inner" -> "anon1_1",
          "anon1_2_inner" -> "anon1_2",
          "anon1_3_inner" -> "anon1_3",
          "anon1_4_inner" -> "anon1_4",
          "anon2_inner" -> "anon2"
        ),
        Set(
          "r1_1_inner" -> "r1",
          "r1_2_inner" -> "r1",
          "r1_3_inner" -> "r1",
          "r1_4_inner" -> "r1",
          "r2_inner" -> "r2"
        ),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.ShortestGroups(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("s", topLeftLabel)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val rowEnd = TrailTestBase.listOf(nodes(4), nodes(5), nodes(14), nodes(15))
    val row = TrailTestBase.listOf(
      nodes(0),
      nodes(1),
      nodes(2),
      nodes(3),
      nodes(9),
      nodes(8),
      nodes(7),
      nodes(6),
      nodes(10),
      nodes(11),
      nodes(12),
      nodes(13),
      nodes(19),
      nodes(18),
      nodes(17),
      nodes(16),
      nodes(20),
      nodes(21),
      nodes(22),
      nodes(23)
    )
    val expected = Seq(Array[Object](
      nodes.head,
      row,
      rowEnd,
      nodes.last
    ))

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test(
    "grid graph - top left to bottom right, where we need to go traverse entire rows before we go one step down - ExpandInto"
  ) {

    // This test tests propagation
    val dim = 5 // Don't change
    val topLeftLabel = s"0,0"
    val bottomRightLabel = s"${dim - 1},${dim - 1}"

    val nodes = givenGraph {
      val (nodes, _) = gridGraph(dim, dim)
      nodes
    }

    // pattern:
    //
    // (s: $topLeftLabel) ( ((row)-[r1:RIGHT]-()){5} (rowEnd)-[r2:DOWN]->()? )+ (t: $bottomRightLabel)
    //
    // NOTE - The nfa is structured as if the inner quantification was explicitly expanded, but that is tedious
    // to write, so we simplify it's description with {5}. Even if nested group variables (or really, quantification's)
    // aren't allowed in cypher, we can collect all row vars into one group here if we want.

    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (row_1_inner)")
      .addTransition(1, 2, "(row_1_inner)-[r1_1_inner:RIGHT]-(anon1_1_inner)")
      // 1 hop traversed
      .addTransition(2, 3, "(anon1_1_inner) (row_2_inner)")
      .addTransition(3, 4, "(row_2_inner)-[r1_2_inner:RIGHT]-(anon1_2_inner)")
      // 2 hops traversed
      .addTransition(4, 5, "(anon1_2_inner) (row_3_inner)")
      .addTransition(5, 6, "(row_3_inner)-[r1_3_inner:RIGHT]-(anon1_3_inner)")
      // 3 hops traversed
      .addTransition(6, 7, "(anon1_3_inner) (row_4_inner)")
      .addTransition(7, 8, "(row_4_inner)-[r1_4_inner:RIGHT]-(anon1_4_inner)")
      .addTransition(8, 11, s"(anon1_4_inner) (t)") // skip second inner pattern with "?"
      // 5 hops traversed
      .addTransition(8, 9, s"(anon1_4_inner) (rowEnd_inner)")
      .addTransition(9, 10, "(rowEnd_inner)-[r2_inner:DOWN]->(anon2_inner)")
      // - Full outer quantification traversed
      .addTransition(10, 1, "(anon2_inner) (row_1_inner)")
      .addTransition(10, 11, s"(anon2_inner) (t)")
      .setFinalState(11)
      .build()

    val retVars = Seq("s", "row", "rowEnd", "t")
    val vars = Seq("s", "row", "r2", "anon2", "rowEnd", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        s"(s) ( ((row)-[r1:RIGHT]-()){5} (rowEnd)-[r2:DOWN]->() )+ (t)",
        None,
        Set(
          "row_1_inner" -> "row",
          "row_2_inner" -> "row",
          "row_3_inner" -> "row",
          "row_4_inner" -> "row",
          "rowEnd_inner" -> "rowEnd"
        ),
        Set(
          "r1_1_inner" -> "r1",
          "r1_2_inner" -> "r1",
          "r1_3_inner" -> "r1",
          "r1_4_inner" -> "r1",
          "r2_inner" -> "r2"
        ),
        Set.empty,
        Set.empty,
        Selector.ShortestGroups(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("t", bottomRightLabel)
      .nodeByLabelScan("s", topLeftLabel)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val rowEnd = TrailTestBase.listOf(nodes(4), nodes(5), nodes(14), nodes(15))
    val row = TrailTestBase.listOf(
      nodes(0),
      nodes(1),
      nodes(2),
      nodes(3),
      nodes(9),
      nodes(8),
      nodes(7),
      nodes(6),
      nodes(10),
      nodes(11),
      nodes(12),
      nodes(13),
      nodes(19),
      nodes(18),
      nodes(17),
      nodes(16),
      nodes(20),
      nodes(21),
      nodes(22),
      nodes(23)
    )
    val expected = Seq(Array[Object](
      nodes.head,
      row,
      rowEnd,
      nodes.last
    ))

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("grid graph - start anywhere, post filter for hamiltonian paths") {
    val dim = 3

    givenGraph {
      gridGraph(dim, dim)
    }

    // pattern:
    // (s) ((n)-[r1]-())* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n_inner)")
      .addTransition(0, 3, "(s) (t_inner)")
      .addTransition(1, 2, "(n_inner)-[r1_inner]-(anon_inner)")
      .addTransition(2, 1, s"(anon_inner) (n_inner)")
      .addTransition(2, 3, s"(anon_inner) (t_inner)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n", "r1", "t")
    val retVars = Seq("s", "n", "t")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        s"(s) ((n)-[r1]-())* (t)",
        Some(s"size(n) = ${dim * dim - 1} AND ALL(x IN n WHERE SINGLE(y IN n WHERE x=y) AND x<>t) "),
        Set("n_inner" -> "n", "anon_inner" -> "anon"),
        Set("r1_inner" -> "r1"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.ShortestGroups(Int.MaxValue),
        nfa,
        ExpandAll
      )
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then

    // The calculations below are true for directed paths in a 3x3 grid graph, 5x5 is more complicated

    // For each corner we have two directed paths for each of the following shape ((*) is start node, [*] is end node)
    //
    //  (*)- * - *     (reflect along diagonal to get other path)
    //           |
    //   * -[*]  *
    //   |       |
    //   * - * - *
    //
    //  (*)- * - *     (reflect along diagonal to get other path)
    //           |
    //   * - *   *
    //   |   |   |
    //  [*]  * - *
    //
    //  (*)- * - *    (reflect along diagonal to get other path)
    //           |
    //   * - * - *
    //   |
    //   * - * -[*]
    //
    //  (*)- *  [*]   (reflect along diagonal to get other path)
    //       |   |
    //   * - *   *
    //   |       |
    //   * - * - *

    // 4 corners, 8 paths starting in each corner
    val nRowsStartingInACorner = 4 * 8

    // There are no trails starting with side nodes that visit every node exactly once
    val nRowsStartingAtSide = 0

    // Exactly the reverse of the paths starting in corner and ending in middle, 2 for each corner
    val nRowsStartingInTheMiddle = 4 * 2

    val expectedRowCount = nRowsStartingInACorner + nRowsStartingAtSide + nRowsStartingInTheMiddle

    runtimeResult should beColumns(retVars: _*).withRows(rowCount(expectedRowCount))
  }

  test("grid graph - start anywhere, post filter for hamiltonian paths - ExpandInto") {
    val dim = 3

    givenGraph {
      gridGraph(dim, dim)
    }

    // pattern:
    // (s) ((n)-[r1]-())* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n_inner)")
      .addTransition(0, 3, "(s) (t)")
      .addTransition(1, 2, "(n_inner)-[r1_inner]-(anon_inner)")
      .addTransition(2, 1, s"(anon_inner) (n_inner)")
      .addTransition(2, 3, s"(anon_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n", "r1", "t")
    val retVars = Seq("s", "n", "t")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        s"(s) ((n)-[r1]-())* (t)",
        Some(s"size(n) = ${dim * dim - 1} AND ALL(x IN n WHERE SINGLE(y IN n WHERE x=y) AND x<>t) "),
        Set("n_inner" -> "n", "anon_inner" -> "anon"),
        Set("r1_inner" -> "r1"),
        Set.empty,
        Set.empty,
        Selector.ShortestGroups(Int.MaxValue),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.allNodeScan("t")
      .allNodeScan("s")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then

    // The calculations below are true for directed paths in a 3x3 grid graph, 5x5 is more complicated

    // For each corner we have two directed paths for each of the following shape ((*) is start node, [*] is end node)
    //
    //  (*)- * - *     (reflect along diagonal to get other path)
    //           |
    //   * -[*]  *
    //   |       |
    //   * - * - *
    //
    //  (*)- * - *     (reflect along diagonal to get other path)
    //           |
    //   * - *   *
    //   |   |   |
    //  [*]  * - *
    //
    //  (*)- * - *    (reflect along diagonal to get other path)
    //           |
    //   * - * - *
    //   |
    //   * - * -[*]
    //
    //  (*)- *  [*]   (reflect along diagonal to get other path)
    //       |   |
    //   * - *   *
    //   |       |
    //   * - * - *

    // 4 corners, 8 paths starting in each corner
    val nRowsStartingInACorner = 4 * 8

    // There are no trails starting with side nodes that visit every node exactly once
    val nRowsStartingAtSide = 0

    // Exactly the reverse of the paths starting in corner and ending in middle, 2 for each corner
    val nRowsStartingInTheMiddle = 4 * 2

    val expectedRowCount = nRowsStartingInACorner + nRowsStartingAtSide + nRowsStartingInTheMiddle

    runtimeResult should beColumns(retVars: _*).withRows(rowCount(expectedRowCount))
  }

  test("propagation through purged and re-registered rev step") {

    val (n0, n3, n5) = givenGraph {

      val graph = fromTemplate(
        """   .-->(n2)--.
          |   |         v
          | (n0:S)---->(n1)-->(n3:T)-->(n4)-->(n5:T)
          |""".stripMargin
      )

      (graph node "n0", graph node "n3", graph node "n5")
    }

    // pattern:
    // (s) (n1)-[r]->(n2)* (t: T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(0, 3, "(s) (t_inner: T)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner: T)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = Seq("s", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(2),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](n0, n3),
      Array[Object](n0, n3),
      Array[Object](n0, n5),
      Array[Object](n0, n5)
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("propagation through purged and re-registered rev step - ExpandInto") {

    val ns = givenGraph {

      //      (n2)
      //      /   \
      // (n0: S)-->(n1)-->(n3: T)-->(n4)-->(n5: T)

      val ns = nodeGraph(6)

      ns(0).addLabel(Label.label("S"))
      ns(3).addLabel(Label.label("T"))
      ns(5).addLabel(Label.label("T"))

      val R = RelationshipType.withName("R")

      ns(0).createRelationshipTo(ns(1), R)
      ns(1).createRelationshipTo(ns(3), R)
      ns(3).createRelationshipTo(ns(4), R)
      ns(4).createRelationshipTo(ns(5), R)

      ns(0).createRelationshipTo(ns(2), R)
      ns(2).createRelationshipTo(ns(1), R)

      ns
    }

    // pattern:
    // (s) (n1)-[r]->(n2)* (t: T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(0, 3, "(s) (t)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = Seq("s", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set.empty,
        Set.empty,
        Selector.Shortest(2),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("t", "T")
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](ns(0), ns(3)),
      Array[Object](ns(0), ns(3)),
      Array[Object](ns(0), ns(5)),
      Array[Object](ns(0), ns(5))
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("grid graph - top left to bottom right, shortest 2 groups") {
    val dim = 5
    val bottomRightLabel = s"`${dim - 1},${dim - 1}`"
    val topLeftLabel = s"`0,0`"

    givenGraph {
      gridGraph(dim, dim)
    }

    // pattern:
    // (s) ((n1)-[r]-(n2))+ (t: $bottomRightLabel)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]-(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, s"(n2_inner) (t_inner: $bottomRightLabel)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = Seq("s", "r", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        s"(s) ((n1)-[r]-(n2))+ (t: $bottomRightLabel)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.ShortestGroups(2),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("s", "0,0")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // 8 choose 4 = 70, https://en.wikipedia.org/wiki/Lattice_path#Combinations_and_NE_lattice_paths
    val nPathsInFirstGroup = 70

    // The paths in the second group have length 10, and will consist of 4 DOWN rels, 4 RIGHT rels and either an UP rel
    // and an extra DOWN rel, or a LEFT rel and an extra RIGHT rel. As these two cases are symmetric, it will suffice to
    // count the amount of paths with 5 DOWN rels, 4 RIGHT rels, and one UP rel, and then multiply by 2.
    //
    // We have further restrictions. First of, due to the structure of the grid graph and that we're starting
    // in the top left corner, some DOWN rel has to have been traversed at some point before the UP rel. Similarly,
    // some DOWN rel has to be traversed after the UP rel. The second restriction is that we can't go immediately DOWN
    // after we go UP, nor can we go immediately UP after we've gone
    // DOWN, as these two scenarios would violate relationship uniqueness. In summary the types of the path has
    // to match something like
    //
    //   p.rels.map(rel => rel.type) == ..., DOWN, ..., RIGHT, UP, RIGHT, ..., DOWN, ...
    //
    // where we are free to assign the remaining rel types however we wish among the ...'s.
    //
    // We will begin by ignoring the first requirement, that there must be some DOWN rel before/after the RIGHT UP RIGHT
    // pattern. If we do this, there are 5 DOWN rels and 2 RIGHT rels remaining. These can be arranged in a sequence in
    // (7!)/(5!2!) = 21 ways. We can shove the RIGHT, UP, RIGHT, rel pattern between any of the rels in such a sequence
    // (or before/after the whole sequence). This can be done in 8 ways, so there are 8 * 21 = 168 sequences of types
    // matching the
    //
    // ..., RIGHT, UP, RIGHT, ...
    //
    // pattern. Now we need to figure out how many of these there are that have no DOWN rel before/after the UP rel.
    // By symmetry again, it will suffice how many of these paths there are that don't have any DOWN rels before the
    // RIGHT, UP, RIGHT pattern.
    //
    // We count this by first assuming that all 5 DOWN rels are to the right of the RIGHT, UP, RIGHT pattern, like
    //
    // ..., RIGHT, UP, RIGHT, ..., DOWN, ..., DOWN, ..., DOWN, ..., DOWN, ..., DOWN, ...
    //
    // This leaves us to choose between 7 slots to put our remaining two RIGHT rels. This can be done in
    // (7 choose 2) + 7 = 28 ways (the + 7 is for the times where both RIGHT rels are in the same "...").
    //
    // So there are 2 * 28 = 56 sequences that have no DOWN rel before/after the UP rel.
    // I.e there are 168 - 56 = 112 sequences like
    //
    //  ..., DOWN, ..., RIGHT, UP, RIGHT, ..., DOWN, ...
    //
    // and if we account for the
    //
    // ..., RIGHT, ..., DOWN, LEFT, DOWN, ..., RIGHT, ...
    //
    // sequences there are 2 * 112 = 224 in total.

    val nPathsInSecondGroup = 224

    val expected = nPathsInFirstGroup + nPathsInSecondGroup

    runtimeResult should beColumns(retVars: _*).withRows(RowCount(expected))
  }

  test("grid graph - top left to bottom right, shortest 2 groups - ExpandInto") {
    val dim = 5
    val bottomRightLabel = s"${dim - 1},${dim - 1}"
    val topLeftLabel = s"`0,0`"

    givenGraph {
      gridGraph(dim, dim)
    }

    // pattern:
    // (s) ((n1)-[r]-(n2))+ (t: $bottomRightLabel)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]-(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, s"(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = Seq("s", "r", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .statefulShortestPath(
        "s",
        "t",
        s"(s) ((n1)-[r]-(n2))+ (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set.empty,
        Set.empty,
        Selector.ShortestGroups(2),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("t", bottomRightLabel)
      .nodeByLabelScan("s", "0,0")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // 8 choose 4 = 70, https://en.wikipedia.org/wiki/Lattice_path#Combinations_and_NE_lattice_paths
    val nPathsInFirstGroup = 70

    // The paths in the second group have length 10, and will consist of 4 DOWN rels, 4 RIGHT rels and either an UP rel
    // and an extra DOWN rel, or a LEFT rel and an extra RIGHT rel. As these two cases are symmetric, it will suffice to
    // count the amount of paths with 5 DOWN rels, 4 RIGHT rels, and one UP rel, and then multiply by 2.
    //
    // We have further restrictions. First of, due to the structure of the grid graph and that we're starting
    // in the top left corner, some DOWN rel has to have been traversed at some point before the UP rel. Similarly,
    // some DOWN rel has to be traversed after the UP rel. The second restriction is that we can't go immediately DOWN
    // after we go UP, nor can we go immediately UP after we've gone
    // DOWN, as these two scenarios would violate relationship uniqueness. In summary the types of the path has
    // to match something like
    //
    //   p.rels.map(rel => rel.type) == ..., DOWN, ..., RIGHT, UP, RIGHT, ..., DOWN, ...
    //
    // where we are free to assign the remaining rel types however we wish among the ...'s.
    //
    // We will begin by ignoring the first requirement, that there must be some DOWN rel before/after the RIGHT UP RIGHT
    // pattern. If we do this, there are 5 DOWN rels and 2 RIGHT rels remaining. These can be arranged in a sequence in
    // (7!)/(5!2!) = 21 ways. We can shove the RIGHT, UP, RIGHT, rel pattern between any of the rels in such a sequence
    // (or before/after the whole sequence). This can be done in 8 ways, so there are 8 * 21 = 168 sequences of types
    // matching the
    //
    // ..., RIGHT, UP, RIGHT, ...
    //
    // pattern. Now we need to figure out how many of these there are that have no DOWN rel before/after the UP rel.
    // By symmetry again, it will suffice how many of these paths there are that don't have any DOWN rels before the
    // RIGHT, UP, RIGHT pattern.
    //
    // We count this by first assuming that all 5 DOWN rels are to the right of the RIGHT, UP, RIGHT pattern, like
    //
    // ..., RIGHT, UP, RIGHT, ..., DOWN, ..., DOWN, ..., DOWN, ..., DOWN, ..., DOWN, ...
    //
    // This leaves us to choose between 7 slots to put our remaining two RIGHT rels. This can be done in
    // (7 choose 2) + 7 = 28 ways (the + 7 is for the times where both RIGHT rels are in the same "...").
    //
    // So there are 2 * 28 = 56 sequences that have no DOWN rel before/after the UP rel.
    // I.e there are 168 - 56 = 112 sequences like
    //
    //  ..., DOWN, ..., RIGHT, UP, RIGHT, ..., DOWN, ...
    //
    // and if we account for the
    //
    // ..., RIGHT, ..., DOWN, LEFT, DOWN, ..., RIGHT, ...
    //
    // sequences there are 2 * 112 = 224 in total.

    val nPathsInSecondGroup = 224

    val expected = nPathsInFirstGroup + nPathsInSecondGroup

    runtimeResult should beColumns(retVars: _*).withRows(RowCount(expected))
  }

  test("ruthless cobweb") {
    givenGraph {

      fromTemplate(
        """
          |         .--->( )--->( )--->( )
          |         |                   |
          |         |                  [:R]
          |         |    .---[:R]--.    |
          |         |    |         v    v
          |       (:S)->( )->( )->( )->( )->(:T)
          |         |    ^
          |         |    |
          |         |   [:R]
          |         |    |
          |         '-->(:T)
          |
          |""".stripMargin,
        defaultRelType = "NOT_R"
      )
    }

    // pattern:
    // (s) ((n1)-[r1:NOT_R]-(n2))+ (n3)-[r2:R]-(n4) ((n5)-[r3:NOT_R]-(n6))+ (t: T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner:NOT_R]-(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (n3_inner)")
      .addTransition(3, 4, "(n3_inner)-[r2_inner:R]-(n4_inner)")
      .addTransition(4, 5, "(n4_inner) (n5_inner)")
      .addTransition(5, 6, "(n5_inner)-[r3_inner:NOT_R]-(n6_inner)")
      .addTransition(6, 5, "(n6_inner) (n5_inner)")
      .addTransition(6, 7, "(n6_inner) (t_inner: T)")
      .setFinalState(7)
      .build()

    val vars = Seq("s", "n1", "n2", "n3", "n4", "n5", "n6", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n0)-[r1:NOT_R]-(n1))+ (n2)-[r2:R]-(n3) ((n4)-[r3:NOT_R]-(n5))+ (t: T)",
        None,
        Set(
          "n1_inner" -> "n1",
          "n2_inner" -> "n2",
          "n5_inner" -> "n5",
          "n6_inner" -> "n6"
        ),
        Set(
          "r1_inner" -> "r1",
          "r3_inner" -> "r3"
        ),
        Set(
          "n3_inner" -> "n3",
          "n4_inner" -> "n4",
          "t_inner" -> "t"
        ),
        Set("r2_inner" -> "r2"),
        Selector.ShortestGroups(3),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedRowCount = 5
    /*
      (0, 0->1)-[2]-(5, 2->3)-[10]-(7, 4->5)-[7]-(8, 6->5)-[8]-(9, 6->7)
      (0, 0->1)-[0]-(1, 2->1)-[3]-(2, 2->1)-[4]-(3, 2->3)-[9]-(8, 4->5)-[8]-(9, 6->7)
      (0, 0->1)-[1]-(4, 2->3)-[11]-(5, 4->5)-[5]-(6, 6->5)-[6]-(7, 6->5)-[7]-(8, 6->5)-[8]-(9, 6->7)
      (0, 0->1)-[0]-(1, 2->1)-[3]-(2, 2->1)-[4]-(3, 2->3)-[9]-(8, 4->5)-[7]-(7, 6->5)-[6]-(6, 6->5)-[5]-(5, 6->5)-[2]-(0, 6->5)-[1]-(4, 6->7)
      (0, 0->1)-[2]-(5, 2->1)-[5]-(6, 2->1)-[6]-(7, 2->1)-[7]-(8, 2->3)-[9]-(3, 4->5)-[4]-(2, 6->5)-[3]-(1, 6->5)-[0]-(0, 6->5)-[1]-(4, 6->7)
     */
    runtimeResult should beColumns(vars: _*).withRows(RowCount(expectedRowCount))

  }

  test("ruthless cobweb - ExpandIntp") {
    givenGraph {

      //       (n1)--(n2)--(n3)
      //      /                \
      //     /     ,--[:R]--.  [:R]
      //    /     /          \   \
      //  (n0)--(n5)--(n6)--(n7)--(n8)--(t9)
      //    \    |
      //     \  [:R]
      //      \  |
      //      (t4)

      val outRelType = org.neo4j.graphdb.RelationshipType.withName("NOT_R")
      val randomRelType = org.neo4j.graphdb.RelationshipType.withName("R")

      val sourceLabel = Label.label("S")
      val targetLabel = Label.label("T")

      val centerNode = tx.createNode(Label.label("0,0"))
      centerNode.addLabel(sourceLabel)

      val n1 = tx.createNode()
      val n2 = tx.createNode()
      val n3 = tx.createNode()

      val n4 = tx.createNode()
      n4.addLabel(targetLabel)

      val n5 = tx.createNode()
      val n6 = tx.createNode()
      val n7 = tx.createNode()
      val n8 = tx.createNode()
      val n9 = tx.createNode()
      n9.addLabel(targetLabel)

      centerNode.createRelationshipTo(n1, outRelType)
      centerNode.createRelationshipTo(n4, outRelType)
      centerNode.createRelationshipTo(n5, outRelType)

      n1.createRelationshipTo(n2, outRelType)
      n2.createRelationshipTo(n3, outRelType)

      n5.createRelationshipTo(n6, outRelType)
      n6.createRelationshipTo(n7, outRelType)
      n7.createRelationshipTo(n8, outRelType)
      n8.createRelationshipTo(n9, outRelType)

      n8.createRelationshipTo(n3, randomRelType)
      n7.createRelationshipTo(n5, randomRelType)
      n5.createRelationshipTo(n4, randomRelType)
    }

    // pattern:
    // (s) ((n1)-[r1:NOT_R]-(n2))+ (n3)-[r2:R]-(n4) ((n5)-[r3:NOT_R]-(n6))+ (t: T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r1_inner:NOT_R]-(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (n3_inner)")
      .addTransition(3, 4, "(n3_inner)-[r2_inner:R]-(n4_inner)")
      .addTransition(4, 5, "(n4_inner) (n5_inner)")
      .addTransition(5, 6, "(n5_inner)-[r3_inner:NOT_R]-(n6_inner)")
      .addTransition(6, 5, "(n6_inner) (n5_inner)")
      .addTransition(6, 7, "(n6_inner) (t)")
      .setFinalState(7)
      .build()

    val vars = Seq("s", "n1", "n2", "n3", "n4", "n5", "n6", "t")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars: _*)
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n0)-[r1:NOT_R]-(n1))+ (n2)-[r2:R]-(n3) ((n4)-[r3:NOT_R]-(n5))+ (t)",
        None,
        Set(
          "n1_inner" -> "n1",
          "n2_inner" -> "n2",
          "n5_inner" -> "n5",
          "n6_inner" -> "n6"
        ),
        Set(
          "r1_inner" -> "r1",
          "r3_inner" -> "r3"
        ),
        Set(
          "n3_inner" -> "n3",
          "n4_inner" -> "n4"
        ),
        Set("r2_inner" -> "r2"),
        Selector.ShortestGroups(3),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("t", "T")
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedRowCount = 5
    /*
      (0, 0->1)-[2]-(5, 2->3)-[10]-(7, 4->5)-[7]-(8, 6->5)-[8]-(9, 6->7)
      (0, 0->1)-[0]-(1, 2->1)-[3]-(2, 2->1)-[4]-(3, 2->3)-[9]-(8, 4->5)-[8]-(9, 6->7)
      (0, 0->1)-[1]-(4, 2->3)-[11]-(5, 4->5)-[5]-(6, 6->5)-[6]-(7, 6->5)-[7]-(8, 6->5)-[8]-(9, 6->7)
      (0, 0->1)-[0]-(1, 2->1)-[3]-(2, 2->1)-[4]-(3, 2->3)-[9]-(8, 4->5)-[7]-(7, 6->5)-[6]-(6, 6->5)-[5]-(5, 6->5)-[2]-(0, 6->5)-[1]-(4, 6->7)
      (0, 0->1)-[2]-(5, 2->1)-[5]-(6, 2->1)-[6]-(7, 2->1)-[7]-(8, 2->3)-[9]-(3, 4->5)-[4]-(2, 6->5)-[3]-(1, 6->5)-[0]-(0, 6->5)-[1]-(4, 6->7)
     */
    runtimeResult should beColumns(vars: _*).withRows(RowCount(expectedRowCount))

  }

  test("target signpost purging through loops") {

    givenGraph {

      //                     (n5)-->(n6)
      //                        ^  /
      //                         \V
      // (n0:S)-->(n1)-->(n2)-->(n3)-->(n4:T)
      //   |      ^       \
      //   V      |        `->(n7)-->(n8)-->(n9:T)
      //  (n10)  (n17)
      //   |      ^
      //   V      |
      //  (n11)  (n16)
      //   |      ^
      //   V      |
      //  (n12)  (n15)
      //   |      ^
      //   V      |
      //  (n13)-->(n14)

      val R = RelationshipType.withName("R")

      val Seq(n0, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16, n17) = nodeGraph(18)
      n0.addLabel(Label.label("S"))
      n4.addLabel(Label.label("T"))
      n9.addLabel(Label.label("T"))

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n2.createRelationshipTo(n3, R)
      n3.createRelationshipTo(n4, R)

      n2.createRelationshipTo(n7, R)
      n7.createRelationshipTo(n8, R)
      n8.createRelationshipTo(n9, R)

      n3.createRelationshipTo(n5, R)
      n5.createRelationshipTo(n6, R)
      n6.createRelationshipTo(n3, R)

      n0.createRelationshipTo(n10, R)
      n10.createRelationshipTo(n11, R)
      n11.createRelationshipTo(n12, R)
      n12.createRelationshipTo(n13, R)
      n13.createRelationshipTo(n14, R)
      n14.createRelationshipTo(n15, R)
      n15.createRelationshipTo(n16, R)
      n16.createRelationshipTo(n17, R)
      n17.createRelationshipTo(n1, R)
    }

    // pattern:
    // (s) ((n1)-[r]->(n2))* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 3, "(s) (t_inner:T)")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner:T)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = Seq("l")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection("length(p) as l")
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(2),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array(4),
      Array(5),
      Array(7),
      Array(13)
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("target signpost purging through loops - ExpandInto") {

    givenGraph {

      //                     (n5)-->(n6)
      //                        ^  /
      //                         \V
      // (n0:S)-->(n1)-->(n2)-->(n3)-->(n4:T)
      //   |      ^       \
      //   V      |        `->(n7)-->(n8)-->(n9:T)
      //  (n10)  (n17)
      //   |      ^
      //   V      |
      //  (n11)  (n16)
      //   |      ^
      //   V      |
      //  (n12)  (n15)
      //   |      ^
      //   V      |
      //  (n13)-->(n14)

      val R = RelationshipType.withName("R")

      val Seq(n0, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16, n17) = nodeGraph(18)
      n0.addLabel(Label.label("S"))
      n4.addLabel(Label.label("T"))
      n9.addLabel(Label.label("T"))

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n2.createRelationshipTo(n3, R)
      n3.createRelationshipTo(n4, R)

      n2.createRelationshipTo(n7, R)
      n7.createRelationshipTo(n8, R)
      n8.createRelationshipTo(n9, R)

      n3.createRelationshipTo(n5, R)
      n5.createRelationshipTo(n6, R)
      n6.createRelationshipTo(n3, R)

      n0.createRelationshipTo(n10, R)
      n10.createRelationshipTo(n11, R)
      n11.createRelationshipTo(n12, R)
      n12.createRelationshipTo(n13, R)
      n13.createRelationshipTo(n14, R)
      n14.createRelationshipTo(n15, R)
      n15.createRelationshipTo(n16, R)
      n16.createRelationshipTo(n17, R)
      n17.createRelationshipTo(n1, R)
    }

    // pattern:
    // (s) ((n1)-[r]->(n2))* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 3, "(s) (t)")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = Seq("l")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection("length(p) as l")
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set.empty,
        Set.empty,
        Selector.Shortest(2),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("t", "T")
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array(4),
      Array(5),
      Array(7),
      Array(13)
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("target signpost purging with multiple loops on target node") {

    givenGraph {

      fromTemplate(
        """
          |                            ()->()
          |                             ^  |
          |                             |  v
          |    (:S)-->( )-->( )-->( )-->(:T)<-.
          |     |      ^     |             '--'
          |     v      |     |
          |    ( )    ( )    '->()-->()-->()-->(:T)
          |     |      ^
          |     v      |
          |    ( )    ( )
          |     |      ^
          |     v      |
          |    ( )    ( )
          |     |      ^
          |     v      |
          |    ( )--->( )
          |""".stripMargin
      )
    }

    // pattern:
    // (s) ((n1)-[r]->(n2))* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 3, "(s) (t_inner:T)")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t_inner:T)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = Seq("l")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection("length(p) as l")
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set("t_inner" -> "t"),
        Set.empty,
        Selector.Shortest(3),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array(4),
      Array(5),
      Array(7),
      Array(6),
      Array(14)
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("target signpost purging with multiple loops on target node - ExpandInto") {

    givenGraph {

      //                            (n5)-->(n6)
      //                               ^  /
      //                                \V    / \
      // (n0:S)-->(n1)-->(n2)-->(n3)-->(n4:T)´   |
      //   |      ^       \                   \ /
      //   V      |        \
      //  (n10)  (n17)      `->(n7)-->(n8)-->(n18)-->(n9:T)
      //   |      ^
      //   V      |
      //  (n11)  (n16)
      //   |      ^
      //   V      |
      //  (n12)  (n15)
      //   |      ^
      //   V      |
      //  (n13)-->(n14)

      val R = RelationshipType.withName("R")

      val Seq(n0, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16, n17, n18) = nodeGraph(19)
      n0.addLabel(Label.label("S"))
      n4.addLabel(Label.label("T"))
      n9.addLabel(Label.label("T"))

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n2.createRelationshipTo(n3, R)
      n3.createRelationshipTo(n4, R)

      n2.createRelationshipTo(n7, R)
      n7.createRelationshipTo(n8, R)
      n8.createRelationshipTo(n18, R)
      n18.createRelationshipTo(n9, R)

      n4.createRelationshipTo(n5, R)
      n5.createRelationshipTo(n6, R)
      n6.createRelationshipTo(n4, R)

      n4.createRelationshipTo(n4, R)

      n0.createRelationshipTo(n10, R)
      n10.createRelationshipTo(n11, R)
      n11.createRelationshipTo(n12, R)
      n12.createRelationshipTo(n13, R)
      n13.createRelationshipTo(n14, R)
      n14.createRelationshipTo(n15, R)
      n15.createRelationshipTo(n16, R)
      n16.createRelationshipTo(n17, R)
      n17.createRelationshipTo(n1, R)
    }

    // pattern:
    // (s) ((n1)-[r]->(n2))* (t)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 3, "(s) (t)")
      .addTransition(0, 1, "(s) (n1_inner)")
      .addTransition(1, 2, "(n1_inner)-[r_inner]->(n2_inner)")
      .addTransition(2, 1, "(n2_inner) (n1_inner)")
      .addTransition(2, 3, "(n2_inner) (t)")
      .setFinalState(3)
      .build()

    val vars = Seq("s", "n1", "r", "n2", "t")
    val retVars = Seq("l")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection("length(p) as l")
      .projection(Map("p" -> qppPath(varFor("s"), Seq(varFor("n1"), varFor("r")), varFor("t"))))
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((n1)-[r]->(n2))* (t)",
        None,
        Set("n1_inner" -> "n1", "n2_inner" -> "n2"),
        Set("r_inner" -> "r"),
        Set.empty,
        Set.empty,
        Selector.Shortest(3),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("t", "T")
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array(4),
      Array(5),
      Array(7),
      Array(6),
      Array(14)
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("target signpost purging challenger") {

    // This test highlights a problem that any target signpost purging solution we've tried has failed to solve.
    // Namely that after we've purged target a given signpost, which we later re-register towards a new target,
    // we're not guaranteed to register it at it's minimum distance first.

    givenGraph {

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

      val R1 = org.neo4j.graphdb.RelationshipType.withName("R1")
      val R2 = org.neo4j.graphdb.RelationshipType.withName("R2")
      val R3 = org.neo4j.graphdb.RelationshipType.withName("R3")
      val DUP = org.neo4j.graphdb.RelationshipType.withName("DUP")
      val PROBLEM = org.neo4j.graphdb.RelationshipType.withName("PROBLEM")
      val REST = org.neo4j.graphdb.RelationshipType.withName("REST")

      val sourceLabel = Label.label("S")
      val targetLabel = Label.label("T")

      val Seq(n0, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16) = nodeGraph(17)

      // Graph repeated for double checking against creation code:
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

      n0.addLabel(sourceLabel)
      n13.addLabel(targetLabel)
      n16.addLabel(targetLabel)

      n0.createRelationshipTo(n1, DUP) // 0

      n0.createRelationshipTo(n2, R1) // 1
      n2.createRelationshipTo(n1, R1) // 2

      n0.createRelationshipTo(n3, R2) // 3
      n3.createRelationshipTo(n4, R2) // 4
      n4.createRelationshipTo(n1, R2) // 5

      n1.createRelationshipTo(n0, PROBLEM) // 6

      n0.createRelationshipTo(n5, R3) // 7
      n5.createRelationshipTo(n6, R3) // 8
      n6.createRelationshipTo(n7, R3) // 9
      n7.createRelationshipTo(n8, R3) // 10
      n8.createRelationshipTo(n9, R3) // 11
      n9.createRelationshipTo(n10, R3) // 12
      n10.createRelationshipTo(n11, R3) // 13
      n11.createRelationshipTo(n12, R3) // 14
      n12.createRelationshipTo(n1, R3) // 15

      n1.createRelationshipTo(n13, REST) // 16
      n13.createRelationshipTo(n14, REST) // 17
      n14.createRelationshipTo(n15, REST) // 18
      n15.createRelationshipTo(n16, REST) // 19
    }

    // pattern:
    // (s) ((v1)-[r1:DUP|R1|R3]->(v2))+ (v3)-[r2:PROBLEM]->(v4) ((v5)-[r3:DUP|R2]->(v6))+ ((v7)-[r4:REST]->(v8))* (t:T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (v1_inner)")
      .addTransition(1, 2, "(v1_inner)-[r1_inner:DUP|R1|R3]-(v2_inner)")
      .addTransition(2, 1, "(v2_inner) (v1_inner)")
      .addTransition(2, 3, "(v2_inner) (v3_inner)")
      .addTransition(3, 4, "(v3_inner)-[r2_inner:PROBLEM]-(v4_inner)")
      .addTransition(4, 5, "(v4_inner) (v5_inner)")
      .addTransition(5, 6, "(v5_inner)-[r3_inner:DUP|R2]-(v6_inner)")
      .addTransition(6, 5, "(v6_inner) (v5_inner)")
      .addTransition(6, 7, "(v6_inner) (v7_inner)")
      .addTransition(6, 9, "(v6_inner) (t_inner: T)")
      .addTransition(7, 8, "(v7_inner)-[r4_inner:REST]-(v8_inner)")
      .addTransition(8, 7, "(v8_inner) (v7_inner)")
      .addTransition(8, 9, "(v8_inner) (t_inner: T)")
      .setFinalState(9)
      .build()

    val vars = Seq("s", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "t")
    val retVars = Seq("l")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection("size(r1) + 1 + size(r3) + size(r4) as l")
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((v1)-[r1:DUP|R1|R3]->(v2))+ (v3)-[r2:PROBLEM]->(v4) ((v5)-[r3:DUP|R2]->(v6))+ ((v7)-[r4:REST]->(v8))* (t:T)",
        Some("size(r1) + 1 + size(r3) + size(r4) <> 9 AND size(r1) + 1 + size(r3) + size(r4) <> 10"),
        Set(
          "v1_inner" -> "v1",
          "v2_inner" -> "v2",
          "v5_inner" -> "v5",
          "v6_inner" -> "v6",
          "v7_inner" -> "v7",
          "v8_inner" -> "v8"
        ),
        Set(
          "r1_inner" -> "r1",
          "r3_inner" -> "r3",
          "r4_inner" -> "r4"
        ),
        Set(
          "v3_inner" -> "v3",
          "v4_inner" -> "v4",
          "t_inner" -> "t"
        ),
        Set("r2_inner" -> "r2"),
        Selector.Shortest(2),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array(5),
      Array(6),
      Array(8),
      Array(15)
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }

  test("target signpost purging challenger - ExpandInto") {

    // This test highlights a problem that any target signpost purging solution we've tried has failed to solve.
    // Namely that after we've purged target a given signpost, which we later re-register towards a new target,
    // we're not guaranteed to register it at it's minimum distance first.

    givenGraph {

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

      val R1 = org.neo4j.graphdb.RelationshipType.withName("R1")
      val R2 = org.neo4j.graphdb.RelationshipType.withName("R2")
      val R3 = org.neo4j.graphdb.RelationshipType.withName("R3")
      val DUP = org.neo4j.graphdb.RelationshipType.withName("DUP")
      val PROBLEM = org.neo4j.graphdb.RelationshipType.withName("PROBLEM")
      val REST = org.neo4j.graphdb.RelationshipType.withName("REST")

      val sourceLabel = Label.label("S")
      val targetLabel = Label.label("T")

      val Seq(n0, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16) = nodeGraph(17)

      // Graph repeated for double checking against creation code:
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

      n0.addLabel(sourceLabel)
      n13.addLabel(targetLabel)
      n16.addLabel(targetLabel)

      n0.createRelationshipTo(n1, DUP) // 0

      n0.createRelationshipTo(n2, R1) // 1
      n2.createRelationshipTo(n1, R1) // 2

      n0.createRelationshipTo(n3, R2) // 3
      n3.createRelationshipTo(n4, R2) // 4
      n4.createRelationshipTo(n1, R2) // 5

      n1.createRelationshipTo(n0, PROBLEM) // 6

      n0.createRelationshipTo(n5, R3) // 7
      n5.createRelationshipTo(n6, R3) // 8
      n6.createRelationshipTo(n7, R3) // 9
      n7.createRelationshipTo(n8, R3) // 10
      n8.createRelationshipTo(n9, R3) // 11
      n9.createRelationshipTo(n10, R3) // 12
      n10.createRelationshipTo(n11, R3) // 13
      n11.createRelationshipTo(n12, R3) // 14
      n12.createRelationshipTo(n1, R3) // 15

      n1.createRelationshipTo(n13, REST) // 16
      n13.createRelationshipTo(n14, REST) // 17
      n14.createRelationshipTo(n15, REST) // 18
      n15.createRelationshipTo(n16, REST) // 19
    }

    // pattern:
    // (s) ((v1)-[r1:DUP|R1|R3]->(v2))+ (v3)-[r2:PROBLEM]->(v4) ((v5)-[r3:DUP|R2]->(v6))+ ((v7)-[r4:REST]->(v8))* (t:T)
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (v1_inner)")
      .addTransition(1, 2, "(v1_inner)-[r1_inner:DUP|R1|R3]-(v2_inner)")
      .addTransition(2, 1, "(v2_inner) (v1_inner)")
      .addTransition(2, 3, "(v2_inner) (v3_inner)")
      .addTransition(3, 4, "(v3_inner)-[r2_inner:PROBLEM]-(v4_inner)")
      .addTransition(4, 5, "(v4_inner) (v5_inner)")
      .addTransition(5, 6, "(v5_inner)-[r3_inner:DUP|R2]-(v6_inner)")
      .addTransition(6, 5, "(v6_inner) (v5_inner)")
      .addTransition(6, 7, "(v6_inner) (v7_inner)")
      .addTransition(6, 9, "(v6_inner) (t)")
      .addTransition(7, 8, "(v7_inner)-[r4_inner:REST]-(v8_inner)")
      .addTransition(8, 7, "(v8_inner) (v7_inner)")
      .addTransition(8, 9, "(v8_inner) (t)")
      .setFinalState(9)
      .build()

    val vars = Seq("s", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "t")
    val retVars = Seq("l")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(retVars: _*)
      .projection("size(r1) + 1 + size(r3) + size(r4) as l")
      .statefulShortestPath(
        "s",
        "t",
        "(s) ((v1)-[r1:DUP|R1|R3]->(v2))+ (v3)-[r2:PROBLEM]->(v4) ((v5)-[r3:DUP|R2]->(v6))+ ((v7)-[r4:REST]->(v8))* (t)",
        Some("size(r1) + 1 + size(r3) + size(r4) <> 9 AND size(r1) + 1 + size(r3) + size(r4) <> 10"),
        Set(
          "v1_inner" -> "v1",
          "v2_inner" -> "v2",
          "v5_inner" -> "v5",
          "v6_inner" -> "v6",
          "v7_inner" -> "v7",
          "v8_inner" -> "v8"
        ),
        Set(
          "r1_inner" -> "r1",
          "r3_inner" -> "r3",
          "r4_inner" -> "r4"
        ),
        Set(
          "v3_inner" -> "v3",
          "v4_inner" -> "v4"
        ),
        Set("r2_inner" -> "r2"),
        Selector.Shortest(2),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("t", "T")
      .nodeByLabelScan("s", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array(5),
      Array(6),
      Array(8),
      Array(15)
    )

    runtimeResult should beColumns(retVars: _*).withRows(expected)
  }
}
