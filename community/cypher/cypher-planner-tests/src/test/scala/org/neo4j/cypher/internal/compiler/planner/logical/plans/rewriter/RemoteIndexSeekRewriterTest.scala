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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.configuration.GraphDatabaseInternalSettings.RemoteNodeIndexWriteOperators
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

class RemoteIndexSeekRewriterTest extends CypherPlannerTestSuite {

  private def rewrite(
    plan: LogicalPlan,
    writeConfig: Set[RemoteNodeIndexWriteOperators] = Set.empty,
    rewriteNodes: Boolean = true,
    rewriteRelationships: Boolean = true
  ): LogicalPlan =
    plan.endoRewrite(RemoteIndexSeekRewriter(
      writeConfig,
      rewriteNodes = rewriteNodes,
      rewriteRelationships = rewriteRelationships
    ))

  test("should rewrite NodeIndexSeek on RHS of Apply to RemoteNodeIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expand("(m)-[:KNOWS]->(n)")
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result = rewrite(input)

    val expected = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expand("(m)-[:KNOWS]->(n)")
      .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    result shouldEqual expected
  }

  test("should not rewrite NodeIndexSeek on RHS of a hash join where the argumentIds are empty") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeHashJoin("m")
      .|.nodeIndexOperator("n:Person(id = 42)")
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite a top-level NodeIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite NodeIndexSeek on the LHS of an apply plan") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.allNodeScan("m")
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    rewrite(input) shouldEqual input
  }

  test("should rewrite NodeUniqueIndexSeek on RHS of Apply to RemoteNodeUniqueIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expand("(m)-[:KNOWS]->(n)")
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val expected = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expand("(m)-[:KNOWS]->(n)")
      .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input) shouldEqual expected
  }

  test("should not rewrite a top-level NodeUniqueIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeIndexOperator("n:Person(id = 42)", unique = true)
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite NodeUniqueIndexSeek on the LHS of an apply plan") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.allNodeScan("m")
      .nodeIndexOperator("n:Person(id = 42)", unique = true)
      .build()

    rewrite(input) shouldEqual input
  }

  test(
    "should only rewrite NodeIndexSeek on RHS of a Merge-Apply when NON_LOCKING is added to config remote_node_index_write_operators"
  ) {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result = rewrite(input)
    val result_UIL = rewrite(input, Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING))
    val result_CIT = rewrite(input, Set(RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS))
    val result_UIL_CIT = rewrite(
      input,
      Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING, RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS)
    )

    result shouldEqual input
    result_UIL shouldEqual input
    result_CIT shouldEqual input
    result_UIL_CIT shouldEqual input

    val rewritten = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))
    val result_NL_UIL =
      rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING, RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING))
    val result_NL_CIT =
      rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING, RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS))
    val result_NL_UIL_CIT = rewrite(
      input,
      Set(
        RemoteNodeIndexWriteOperators.NON_LOCKING,
        RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING,
        RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS
      )
    )

    result_NL shouldEqual rewritten
    result_NL_UIL shouldEqual rewritten
    result_NL_CIT shouldEqual rewritten
    result_NL_UIL_CIT shouldEqual rewritten
  }

  test(
    "should only rewrite NodeUniqueIndexSeek on RHS of a Merge-Apply when UNIQUE_INDEX_LOCKING is added to config remote_node_index_write_operators"
  ) {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result = rewrite(input)
    val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))
    val result_CIT = rewrite(input, Set(RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS))
    val result_NL_CIT =
      rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING, RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS))

    result shouldEqual input
    result_NL shouldEqual input
    result_CIT shouldEqual input
    result_NL_CIT shouldEqual input

    val rewritten = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result_UIL = rewrite(input, Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING))
    val result_UIL_NL =
      rewrite(input, Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING, RemoteNodeIndexWriteOperators.NON_LOCKING))
    val result_UIL_CIT = rewrite(
      input,
      Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING, RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS)
    )
    val result_UIL_NL_CIT = rewrite(
      input,
      Set(
        RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING,
        RemoteNodeIndexWriteOperators.NON_LOCKING,
        RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS
      )
    )

    result_UIL shouldEqual rewritten
    result_UIL_NL shouldEqual rewritten
    result_UIL_CIT shouldEqual rewritten
    result_UIL_NL_CIT shouldEqual rewritten
  }

  test(
    "should only rewrite NodeIndexSeek on RHS of a CALL IN TRANSACTION when NON_LOCKING and CALL_IN_TRANSACTIONS are added to config remote_node_index_write_operators"
  ) {
    def run(input: LogicalPlan, rewritten: LogicalPlan): Unit = {
      val result = rewrite(input)
      val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))
      val result_UIL = rewrite(input, Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING))
      val result_CIT = rewrite(input, Set(RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS))
      val result_UIL_CIT = rewrite(
        input,
        Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING, RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS)
      )
      val result_NL_UIL =
        rewrite(
          input,
          Set(RemoteNodeIndexWriteOperators.NON_LOCKING, RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING)
        )

      result shouldEqual input
      result_NL shouldEqual input
      result_UIL shouldEqual input
      result_CIT shouldEqual input
      result_UIL_CIT shouldEqual input
      result_NL_UIL shouldEqual input

      val result_NL_CIT =
        rewrite(
          input,
          Set(RemoteNodeIndexWriteOperators.NON_LOCKING, RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS)
        )
      val result_NL_UIL_CIT = rewrite(
        input,
        Set(
          RemoteNodeIndexWriteOperators.NON_LOCKING,
          RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING,
          RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS
        )
      )

      result_NL_CIT shouldEqual rewritten
      result_NL_UIL_CIT shouldEqual rewritten
    }

    withClue("transactionApply") {
      val txApplyInput = new LogicalPlanBuilder(wholePlan = false)
        .transactionApply()
        .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
        .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()

      val txApplyRewritten = new LogicalPlanBuilder(wholePlan = false)
        .transactionApply()
        .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
        .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()
      run(txApplyInput, txApplyRewritten)
    }

    withClue("transactionForeach") {
      val txForeachInput = new LogicalPlanBuilder(wholePlan = false)
        .transactionForeach()
        .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
        .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()

      val txForeachRewritten = new LogicalPlanBuilder(wholePlan = false)
        .transactionForeach()
        .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
        .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()
      run(txForeachInput, txForeachRewritten)
    }
  }

  test(
    "should only rewrite NodeUniqueIndexSeek on RHS of a CALL IN TRANSACTION when UNIQUE_INDEX_LOCKING and CALL_IN_TRANSACTIONS are added to config remote_node_index_write_operators"
  ) {
    def run(input: LogicalPlan, rewritten: LogicalPlan): Unit = {
      val result = rewrite(input)
      val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))
      val result_UIL = rewrite(input, Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING))
      val result_CIT = rewrite(input, Set(RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS))
      val result_NL_CIT =
        rewrite(
          input,
          Set(RemoteNodeIndexWriteOperators.NON_LOCKING, RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS)
        )
      val result_NL_UIL =
        rewrite(
          input,
          Set(RemoteNodeIndexWriteOperators.NON_LOCKING, RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING)
        )

      result shouldEqual input
      result_NL shouldEqual input
      result_UIL shouldEqual input
      result_CIT shouldEqual input
      result_NL_CIT shouldEqual input
      result_NL_UIL shouldEqual input

      val result_UIL_CIT = rewrite(
        input,
        Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING, RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS)
      )
      val result_NL_UIL_CIT = rewrite(
        input,
        Set(
          RemoteNodeIndexWriteOperators.NON_LOCKING,
          RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING,
          RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS
        )
      )

      result_UIL_CIT shouldEqual rewritten
      result_NL_UIL_CIT shouldEqual rewritten
    }

    withClue("transactionApply") {
      val txApplyInput = new LogicalPlanBuilder(wholePlan = false)
        .transactionApply()
        .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
        .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()

      val txApplyRewritten = new LogicalPlanBuilder(wholePlan = false)
        .transactionApply()
        .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
        .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()
      run(txApplyInput, txApplyRewritten)
    }

    withClue("transactionForeach") {
      val txForeachInput = new LogicalPlanBuilder(wholePlan = false)
        .transactionForeach()
        .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
        .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()

      val txForeachRewritten = new LogicalPlanBuilder(wholePlan = false)
        .transactionForeach()
        .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
        .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()
      run(txForeachInput, txForeachRewritten)
    }
  }

  test("should not rewrite NodeIndexSeek on LHS of a Merge-Apply") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.argument("n")
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    val result = rewrite(input)
    val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))

    result shouldEqual input
    result_NL shouldEqual input
  }

  test("should only rewrite NodeIndexSeek AFTER a Merge-Apply when NON_LOCKING is enabled for write queries") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.nodeIndexOperator("m:Person(id = n.id)", argumentIds = Set("n"))
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.argument("n")
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    val result = rewrite(input)
    result shouldEqual input

    val rewritten = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.remoteNodeIndexOperator("m:Person(id = n.id)", argumentIds = Set("n"))
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.argument("n")
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))
    result_NL shouldEqual rewritten
  }

  test(
    "should only rewrite NodeIndexSeek in the RHS of node hash join if there is a write on the other side when NON_LOCKING is enabled for write queries"
  ) {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeHashJoin("m")
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .create(createNode("p", "Person"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result = rewrite(input)
    result shouldEqual input

    val rewritten = new LogicalPlanBuilder(wholePlan = false)
      .nodeHashJoin("m")
      .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .create(createNode("p", "Person"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))
    result_NL shouldEqual rewritten
  }

  test("should not rewrite NodeIndexSeek in the LHS of node hash join") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeHashJoin("m")
      .|.create(createNode("p", "Person"))
      .|.nodeByLabelScan("m", "Person", IndexOrderNone)
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    val result = rewrite(input)
    val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))

    result shouldEqual input
    result_NL shouldEqual input
  }

  test("should rewrite DirectedRelationshipIndexSeek on RHS of Apply to RemoteDirectedRelationshipIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:R(prop = m.prop)]->(y)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val expected = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.remoteRelationshipIndexOperator("(x)-[r:R(prop = m.prop)]->(y)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input) shouldEqual expected
  }

  test("should rewrite UndirectedRelationshipIndexSeek on RHS of Apply to RemoteUndirectedRelationshipIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:R(prop = m.prop)]-(y)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val expected = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.remoteRelationshipIndexOperator("(x)-[r:R(prop = m.prop)]-(y)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input) shouldEqual expected
  }

  test("should not rewrite DirectedRelationshipIndexSeek when the argumentIds are empty") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .relationshipIndexOperator("(x)-[r:R(prop = 42)]->(y)")
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite UndirectedRelationshipIndexSeek when the argumentIds are empty") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .relationshipIndexOperator("(x)-[r:R(prop = 42)]-(y)")
      .build()

    rewrite(input) shouldEqual input
  }

  test(
    "should rewrite DirectedRelationshipUniqueIndexSeek on RHS of Apply to RemoteDirectedRelationshipUniqueIndexSeek"
  ) {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:R(prop = m.prop)]->(y)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val expected = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.remoteRelationshipIndexOperator("(x)-[r:R(prop = m.prop)]->(y)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input) shouldEqual expected
  }

  test(
    "should rewrite UndirectedRelationshipUniqueIndexSeek on RHS of Apply to RemoteUndirectedRelationshipUniqueIndexSeek"
  ) {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:R(prop = m.prop)]-(y)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val expected = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.remoteRelationshipIndexOperator("(x)-[r:R(prop = m.prop)]-(y)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input) shouldEqual expected
  }

  test("should not rewrite DirectedRelationshipUniqueIndexSeek when the argumentIds are empty") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .relationshipIndexOperator("(x)-[r:R(prop = 42)]->(y)", unique = true)
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite DirectedRelationshipIndexSeek on RHS of a Merge-Apply") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.relationshipIndexOperator("(x)-[r:R(prop = m.prop)]->(y)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result = rewrite(input)

    result shouldEqual input
  }

  test("should not rewrite UndirectedRelationshipUniqueIndexSeek on RHS of a Merge-Apply") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.relationshipIndexOperator("(x)-[r:R(prop = m.prop)]-(y)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite relationship seeks when rewriteRelationships = false") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:R(prop = m.prop)]->(y)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input, rewriteRelationships = false) shouldEqual input
  }

  test("should not rewrite node seeks when rewriteNodes = false") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expand("(m)-[:KNOWS]->(n)")
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input, rewriteNodes = false) shouldEqual input
  }

  test(
    "should rewrite MergeUniqueNode on RHS of Apply to RemoteMergeUniqueNode when MERGE_UNIQUE_NODE is added to config remote_node_index_write_operators"
  ) {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.mergeUniqueNode("n", "Person", Seq("id" -> "m.id"), args = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result = rewrite(input)
    val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))
    val result_UIL = rewrite(input, Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING))

    result shouldEqual input
    result_NL shouldEqual input
    result_UIL shouldEqual input

    val rewritten = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.remoteMergeUniqueNode("n", "Person", Seq("id" -> "m.id"), args = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result_MUN = rewrite(input, Set(RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE))
    val result_MUN_NL_UIL = rewrite(
      input,
      Set(
        RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE,
        RemoteNodeIndexWriteOperators.NON_LOCKING,
        RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING
      )
    )

    result_MUN shouldEqual rewritten
    result_MUN_NL_UIL shouldEqual rewritten
  }

  test(
    "should only rewrite MergeUniqueNode on RHS of a CALL IN TRANSACTION when MERGE_UNIQUE_NODE and CALL_IN_TRANSACTIONS are added to config remote_node_index_write_operators"
  ) {
    def run(input: LogicalPlan, rewritten: LogicalPlan): Unit = {
      val result = rewrite(input)
      val result_NL = rewrite(input, Set(RemoteNodeIndexWriteOperators.NON_LOCKING))
      val result_UIL = rewrite(input, Set(RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING))
      val result_MUN = rewrite(input, Set(RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE))
      val result_CIT = rewrite(input, Set(RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS))
      val result_MUN_NL_UIL =
        rewrite(
          input,
          Set(
            RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE,
            RemoteNodeIndexWriteOperators.NON_LOCKING,
            RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING
          )
        )

      result shouldEqual input
      result_NL shouldEqual input
      result_UIL shouldEqual input
      result_MUN shouldEqual input
      result_CIT shouldEqual input
      result_MUN_NL_UIL shouldEqual input

      val result_MUN_CIT = rewrite(
        input,
        Set(RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE, RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS)
      )
      val result_MUN_NL_UIL_CIT = rewrite(
        input,
        Set(
          RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE,
          RemoteNodeIndexWriteOperators.NON_LOCKING,
          RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING,
          RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS
        )
      )

      result_MUN_CIT shouldEqual rewritten
      result_MUN_NL_UIL_CIT shouldEqual rewritten
    }

    withClue("transactionApply") {
      val txApplyInput = new LogicalPlanBuilder(wholePlan = false)
        .transactionApply()
        .|.mergeUniqueNode("n", "Person", Seq("id" -> "m.id"), args = Set("m"))
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()

      val txApplyRewritten = new LogicalPlanBuilder(wholePlan = false)
        .transactionApply()
        .|.remoteMergeUniqueNode("n", "Person", Seq("id" -> "m.id"), args = Set("m"))
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()
      run(txApplyInput, txApplyRewritten)
    }

    withClue("transactionForeach") {
      val txForeachInput = new LogicalPlanBuilder(wholePlan = false)
        .transactionForeach()
        .|.mergeUniqueNode("n", "Person", Seq("id" -> "m.id"), args = Set("m"))
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()

      val txForeachRewritten = new LogicalPlanBuilder(wholePlan = false)
        .transactionForeach()
        .|.remoteMergeUniqueNode("n", "Person", Seq("id" -> "m.id"), args = Set("m"))
        .nodeByLabelScan("m", "Person", IndexOrderNone)
        .build()
      run(txForeachInput, txForeachRewritten)
    }
  }

  test("should not rewrite a top-level MergeUniqueNode even with MERGE_UNIQUE_NODE enabled") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .mergeUniqueNode("n", "Person", Seq("id" -> "42"))
      .build()

    rewrite(input, Set(RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE)) shouldEqual input
  }

  test("should not rewrite MergeUniqueNode when rewriteNodes = false") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.mergeUniqueNode("n", "Person", Seq("id" -> "m.id"), args = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(
      input,
      writeConfig = Set(RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE),
      rewriteNodes = false
    ) shouldEqual input
  }

  test("should rewrite only node seeks when rewriteRelationships = false") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.apply()
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = m.prop)]->(y)", argumentIds = Set("m"))
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val expected = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.apply()
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = m.prop)]->(y)", argumentIds = Set("m"))
      .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input, rewriteRelationships = false) shouldEqual expected
  }
}
