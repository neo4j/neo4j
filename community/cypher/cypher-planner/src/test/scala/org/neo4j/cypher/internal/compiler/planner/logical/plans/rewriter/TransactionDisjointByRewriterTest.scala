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

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionForeach

class TransactionDisjointByRewriterTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  test("should rewrite concurrent TransactionApply with unique index seek + setNodeProperties") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .transactionApply(batchSize = 10, concurrency = 4)
      .|.setNodeProperties("x", "prop" -> "42")
      .|.nodeIndexOperator("x:X(id = ???)", paramExpr = Some(varFor("p")), unique = true, argumentIds = Set("p"))
      .unwind("[1, 2, 3] AS p")
      .argument()
      .build()

    val rewritten = rewrite(plan)
    disjointByOf(rewritten) should not be empty
  }

  test("should rewrite concurrent TransactionForeach with MergeUniqueNode") {
    val plan = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .transactionForeach(batchSize = 10, concurrency = TransactionConcurrency.Concurrent(Some(literalInt(4))))
      .|.mergeUniqueNode("x", "X", Seq("id" -> "p"), args = Set("p"))
      .unwind("[1, 2, 3] AS p")
      .argument()
      .build()

    val rewritten = rewrite(plan)
    disjointByOf(rewritten) should not be empty
  }

  test("should not rewrite serial TransactionApply") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .transactionApply()
      .|.setNodeProperties("x", "prop" -> "42")
      .|.nodeIndexOperator("x:X(id = ???)", paramExpr = Some(varFor("p")), unique = true, argumentIds = Set("p"))
      .unwind("[1, 2, 3] AS p")
      .argument()
      .build()

    rewrite(plan) should equal(plan)
  }

  test("should not rewrite when unique seek is a range seek (non-exact)") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .transactionApply(batchSize = 10, concurrency = 4)
      .|.setNodeProperties("x", "prop" -> "42")
      .|.nodeIndexOperator("x:X(id > 10)", unique = true)
      .unwind("[1, 2, 3] AS p")
      .argument()
      .build()

    rewrite(plan) should equal(plan)
  }

  test("should not rewrite when RHS contains a generic SetProperty (deny-listed)") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .transactionApply(batchSize = 10, concurrency = 4)
      .|.setProperty("x", "prop", "42")
      .|.nodeIndexOperator("x:X(id = ???)", paramExpr = Some(varFor("p")), unique = true, argumentIds = Set("p"))
      .unwind("[1, 2, 3] AS p")
      .argument()
      .build()

    rewrite(plan) should equal(plan)
  }

  test("should not rewrite when seek value depends on RHS-local variable") {
    // `inner` is bound inside the RHS by allNodeScan and is not in lhs.availableSymbols,
    // so the seek's usedVariables are not a subset of the outer inputVars.
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .transactionApply(batchSize = 10, concurrency = 4)
      .|.setNodeProperties("x", "prop" -> "42")
      .|.apply()
      .|.|.nodeIndexOperator(
        "x:X(id = ???)",
        paramExpr = Some(prop("inner", "id")),
        unique = true,
        argumentIds = Set("inner")
      )
      .|.allNodeScan("inner")
      .unwind("[1, 2, 3] AS p")
      .argument()
      .build()

    rewrite(plan) should equal(plan)
  }

  test("should rewrite when Create relationship uses two RAID-identified endpoints") {
    val plan = new LogicalPlanBuilder()
      .produceResults("r")
      .transactionApply(batchSize = 10, concurrency = 4)
      .|.create(createRelationship("r", "a", "R", "b"))
      .|.apply()
      .|.|.nodeIndexOperator(
        "b:B(id = ???)",
        paramExpr = Some(varFor("q")),
        unique = true,
        argumentIds = Set("a", "p", "q")
      )
      .|.nodeIndexOperator(
        "a:A(id = ???)",
        paramExpr = Some(varFor("p")),
        unique = true,
        argumentIds = Set("p", "q")
      )
      .unwind("[1, 2, 3] AS q")
      .unwind("[1, 2, 3] AS p")
      .argument()
      .build()

    val rewritten = rewrite(plan)
    disjointByOf(rewritten) should not be empty
  }

  test("should rewrite when MergeInto between RAID-identified endpoints is followed by SetRelationshipProperties") {
    // MergeInto in the match-case does not itself lock the relationship, but batching by the
    // endpoint RAID values already serializes any two batches that touch the same (A, B) pair,
    // which is sufficient to protect any relationship between them from concurrent modification.
    val plan = new LogicalPlanBuilder()
      .produceResults("r")
      .transactionApply(batchSize = 10, concurrency = 4)
      .|.setRelationshipProperties("r", "prop" -> "42")
      .|.mergeInto("(a)-[r:R]->(b)")
      .|.apply()
      .|.|.nodeIndexOperator(
        "b:B(id = ???)",
        paramExpr = Some(varFor("q")),
        unique = true,
        argumentIds = Set("a", "p", "q")
      )
      .|.nodeIndexOperator(
        "a:A(id = ???)",
        paramExpr = Some(varFor("p")),
        unique = true,
        argumentIds = Set("p", "q")
      )
      .unwind("[1, 2, 3] AS q")
      .unwind("[1, 2, 3] AS p")
      .argument()
      .build()

    val rewritten = rewrite(plan)
    disjointByOf(rewritten) should not be empty
  }

  test("should not rewrite when updater touches node not identified by a unique seek") {
    // `y` is from allNodeScan, not a unique seek. The rewriter must deny.
    val plan = new LogicalPlanBuilder()
      .produceResults("x", "y")
      .transactionApply(batchSize = 10, concurrency = 4)
      .|.setNodeProperties("y", "prop" -> "42")
      .|.apply()
      .|.|.allNodeScan("y", "x", "p")
      .|.nodeIndexOperator(
        "x:X(id = ???)",
        paramExpr = Some(varFor("p")),
        unique = true,
        argumentIds = Set("p")
      )
      .unwind("[1, 2, 3] AS p")
      .argument()
      .build()

    rewrite(plan) should equal(plan)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(TransactionDisjointByRewriter(globalStrategyIsAuto = true))

  private def disjointByOf(plan: LogicalPlan): Seq[Expression] = {
    plan.folder.treeCollect {
      case t: TransactionApply   => t.effectiveDisjointBy
      case t: TransactionForeach => t.effectiveDisjointBy
    }.flatten
  }
}
