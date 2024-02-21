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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanResolver
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanTestOps
import org.neo4j.cypher.internal.compiler.planner.ProcedureTestSupport
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ListSet

class EagerEverywhereRewriterTest extends CypherFunSuite with LogicalPlanTestOps with ProcedureTestSupport {

  private def eagerizePlan(planBuilder: LogicalPlanBuilder, plan: LogicalPlan): LogicalPlan =
    EagerEverywhereRewriter(Attributes(planBuilder.idGen)).eagerize(
      plan,
      planBuilder.getSemanticTable,
      new AnonymousVariableNameGenerator
    )

  test("inserts no eager in linear read query") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .limit(5)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts no eager in branched read query") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .limit(5)
      .apply()
      .|.filter("n.prop > 5")
      .|.argument("n")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager between read and write plans") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .create(createNode("m"))
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .create(createNode("m"))
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between write and write plans") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .setNodeProperty("m", "prop", "5")
      .create(createNode("m"))
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .create(createNode("m"))
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .allNodeScan("n")
        .build()
    )
  }

  test("does not insert eager between read plans in plan that contains writes") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .projection("m.prop as mprop")
      .setProperty("m", "prop", "4")
      .create(createNode("m"))
      .filter("n.prop > 5")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .projection("m.prop as mprop")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .setProperty("m", "prop", "4")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .create(createNode("m"))
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .filter("n.prop > 5")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between binary read plans and write plans") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .valueHashJoin("m.prop = z.foo")
      .|.create(createNode("z"))
      .|.argument()
      .nodeHashJoin("m")
      .|.allNodeScan("m")
      .create(createNode("m"))
      .cartesianProduct()
      .|.allNodeScan("q")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .valueHashJoin("m.prop = z.foo")
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.create(createNode("z"))
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.argument()
        .nodeHashJoin("m")
        .|.allNodeScan("m")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .create(createNode("m"))
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .cartesianProduct()
        .|.allNodeScan("q")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager before and after apply plan if RHS has updates") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.filter("m.prop = 0")
      .|.create(createNode("m"))
      .|.argument("n")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .apply()
        .|.filter("m.prop = 0")
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.create(createNode("m"))
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.argument("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager before and after apply plan if RHS has write procedure call") {
    val resolver = new LogicalPlanResolver(
      procedures = Set(
        procedureSignature("my.writeProc")
          .withAccessMode(ProcedureReadWriteAccess)
          .build()
      )
    )
    val planBuilder = new LogicalPlanBuilder(resolver = resolver)
      .produceResults("n")
      .apply()
      .|.filter("m.prop = 0")
      .|.procedureCall("my.writeProc()")
      .|.argument("n")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder(resolver = resolver)
        .produceResults("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .apply()
        .|.filter("m.prop = 0")
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.procedureCall("my.writeProc()")
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.argument("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between apply plan and LHS updates") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.filter("m.prop = 0")
      .|.argument("m")
      .create(createNode("m"))
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .apply()
        .|.filter("m.prop = 0")
        .|.argument("m")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .create(createNode("m"))
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between apply plan and RHS updates") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.create(createNode("m"))
      .|.filter("n.prop = 0")
      .|.argument("n")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .apply()
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.create(createNode("m"))
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.filter("n.prop = 0")
        .|.argument("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts no duplicate eager around apply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .deleteNode("n")
      .apply()
      .|.create(createNode("m"))
      .|.filter("n.prop = 0")
      .|.argument("n")
      .setNodeProperty("n", "prop", "8")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .deleteNode("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .apply()
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.create(createNode("m"))
        .|.eager(ListSet(EagernessReason.UpdateStrategyEager))
        .|.filter("n.prop = 0")
        .|.argument("n")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .setNodeProperty("n", "prop", "8")
        .eager(ListSet(EagernessReason.UpdateStrategyEager))
        .allNodeScan("n")
        .build()
    )
  }
}
