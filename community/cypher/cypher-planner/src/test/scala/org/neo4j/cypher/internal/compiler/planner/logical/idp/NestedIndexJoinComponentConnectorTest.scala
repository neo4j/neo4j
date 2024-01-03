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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexSeek.relationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.BitSet

class NestedIndexJoinComponentConnectorTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

  private def register[X](registry: IdRegistry[X], elements: X*): Goal = Goal(registry.registerAll(elements))

  private val singleComponentPlanner = SingleComponentPlanner()(mock[IDPQueryGraphSolverMonitor])

  test("produces nested index joins of two components connected by property equality") {
    val table = IDPTable.empty[LogicalPlan]
    val registry: DefaultIdRegistry[QueryGraph] = IdRegistry[QueryGraph]

    val nProp = prop("n", "prop")
    val mProp = prop("m", "prop")
    val joinPred = equals(nProp, mProp)
    val labelNPred = hasLabels("n", "N")
    val labelMPred = hasLabels("m", "M")
    new givenConfig() {
      indexOn("N", "prop")
      indexOn("M", "prop")
      addTypeToSemanticTable(nProp, CTAny)
      addTypeToSemanticTable(mProp, CTAny)
    }.withLogicalPlanningContext { (_, ctx) =>
      val order = InterestingOrderConfig.empty
      val kit = ctx.plannerState.config.toKit(order, ctx)
      val nQg = QueryGraph(patternNodes = Set(v"n")).addPredicates(labelNPred)
      val mQg = QueryGraph(patternNodes = Set(v"m")).addPredicates(labelMPred)
      val fullQg = (nQg ++ mQg).addPredicates(joinPred)

      val nPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "n")
      val mPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "m")
      ctx.staticComponents.planningAttributes.solveds.set(nPlan.id, RegularSinglePlannerQuery(nQg))
      ctx.staticComponents.planningAttributes.solveds.set(mPlan.id, RegularSinglePlannerQuery(mQg))
      table.put(register(registry, nQg), sorted = false, nPlan)
      table.put(register(registry, mQg), sorted = false, mPlan)
      val goal = register(registry, nQg, mQg)

      val step = NestedIndexJoinComponentConnector(singleComponentPlanner).solverStep(
        GoalBitAllocation(2, 0, Seq.empty),
        fullQg,
        order,
        kit,
        ctx
      )
      val plans = step(registry, goal, table, ctx).toSeq
      plans should contain theSameElementsAs Seq(
        Apply(
          mPlan,
          nodeIndexSeek(
            "n:N(prop = ???)",
            _ => CanGetValue,
            paramExpr = Some(mProp),
            argumentIds = Set("m"),
            labelId = 0
          )
        ),
        Apply(
          nPlan,
          nodeIndexSeek(
            "m:M(prop = ???)",
            _ => CanGetValue,
            paramExpr = Some(nProp),
            argumentIds = Set("n"),
            labelId = 1
          )
        )
      )
    }
  }

  test("produces nested relationship index joins of two components connected by property equality") {
    val table = IDPTable.empty[LogicalPlan]
    val registry: DefaultIdRegistry[QueryGraph] = IdRegistry[QueryGraph]

    val nProp = prop("n", "prop")
    val mProp = prop("m", "prop")
    val joinPred = equals(nProp, mProp)
    new givenConfig() {
      relationshipIndexOn("N", "prop")
      relationshipIndexOn("M", "prop")
      addTypeToSemanticTable(nProp, CTAny)
      addTypeToSemanticTable(mProp, CTAny)
    }.withLogicalPlanningContext { (_, ctx) =>
      val order = InterestingOrderConfig.empty
      val kit = ctx.plannerState.config.toKit(order, ctx)
      val nQg = QueryGraph(patternRelationships =
        Set(PatternRelationship(v"n", (v"a", v"b"), BOTH, Seq(relTypeName("N")), SimplePatternLength))
      )
      val mQg = QueryGraph(patternRelationships =
        Set(PatternRelationship(v"m", (v"c", v"d"), BOTH, Seq(relTypeName("M")), SimplePatternLength))
      )
      val fullQg = (nQg ++ mQg).addPredicates(joinPred)

      val nPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "n", "a", "b")
      val mPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "m", "c", "d")
      ctx.staticComponents.planningAttributes.solveds.set(nPlan.id, RegularSinglePlannerQuery(nQg))
      ctx.staticComponents.planningAttributes.solveds.set(mPlan.id, RegularSinglePlannerQuery(mQg))
      table.put(register(registry, nQg), sorted = false, nPlan)
      table.put(register(registry, mQg), sorted = false, mPlan)
      val goal = register(registry, nQg, mQg)

      val step = NestedIndexJoinComponentConnector(singleComponentPlanner).solverStep(
        GoalBitAllocation(2, 0, Seq.empty),
        fullQg,
        order,
        kit,
        ctx
      )
      val plans = step(registry, goal, table, ctx).toSeq
      plans should contain theSameElementsAs Seq(
        Apply(
          mPlan,
          relationshipIndexSeek(
            "(a)-[n:N(prop = ???)]-(b)",
            _ => CanGetValue,
            paramExpr = Some(mProp),
            argumentIds = Set("m", "c", "d"),
            typeId = 0
          )
        ),
        Apply(
          nPlan,
          relationshipIndexSeek(
            "(c)-[m:M(prop = ???)]-(d)",
            _ => CanGetValue,
            paramExpr = Some(nProp),
            argumentIds = Set("n", "a", "b"),
            typeId = 1
          )
        )
      )
    }
  }

  test("produces no nested index joins if idp table is compacted and no single components remain") {
    val table = IDPTable.empty[LogicalPlan]
    val registry: DefaultIdRegistry[QueryGraph] = IdRegistry[QueryGraph]

    val nProp = prop("n", "prop")
    val mProp = prop("m", "prop")
    val oProp = prop("p", "prop")
    val pProp = prop("p", "prop")
    val joinPred = equals(nProp, mProp)
    val labelNPred = hasLabels("n", "N")
    val labelMPred = hasLabels("m", "M")
    val labelOPred = hasLabels("o", "P")
    val labelPPred = hasLabels("p", "P")
    new givenConfig() {
      indexOn("N", "prop")
      indexOn("M", "prop")
      indexOn("O", "prop")
      indexOn("P", "prop")
      addTypeToSemanticTable(nProp, CTAny)
      addTypeToSemanticTable(mProp, CTAny)
      addTypeToSemanticTable(oProp, CTAny)
      addTypeToSemanticTable(pProp, CTAny)
    }.withLogicalPlanningContext { (_, ctx) =>
      val order = InterestingOrderConfig.empty
      val kit = ctx.plannerState.config.toKit(order, ctx)
      val nQg = QueryGraph(patternNodes = Set(v"n")).addPredicates(labelNPred)
      val mQg = QueryGraph(patternNodes = Set(v"m")).addPredicates(labelMPred)
      val oQg = QueryGraph(patternNodes = Set(v"o")).addPredicates(labelOPred)
      val pQg = QueryGraph(patternNodes = Set(v"p")).addPredicates(labelPPred)
      val fullQg = (nQg ++ mQg ++ oQg ++ pQg).addPredicates(joinPred)

      val noPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "n", "o")
      val mpPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "m", "p")
      ctx.staticComponents.planningAttributes.solveds.set(noPlan.id, RegularSinglePlannerQuery(nQg ++ oQg))
      ctx.staticComponents.planningAttributes.solveds.set(mpPlan.id, RegularSinglePlannerQuery(mQg ++ pQg))

      // Register single plans
      val nId = registry.register(nQg)
      val mId = registry.register(mQg)
      val oId = registry.register(oQg)
      val pId = registry.register(pQg)

      // Compact no and mp
      val noId = registry.compact(BitSet(nId, oId))
      table.put(Goal(BitSet(noId)), sorted = false, noPlan)
      val mpId = registry.compact(BitSet(mId, pId))
      table.put(Goal(BitSet(mpId)), sorted = false, mpPlan)

      val goal = Goal(BitSet(noId, mpId))

      val step = NestedIndexJoinComponentConnector(singleComponentPlanner).solverStep(
        GoalBitAllocation(4, 0, Seq.empty),
        fullQg,
        order,
        kit,
        ctx
      )
      val plans = step(registry, goal, table, ctx).toSeq
      plans should be(empty)
    }
  }

  test("does not produce NIJs for query graphs that contain optional matches") {
    val table = IDPTable.empty[LogicalPlan]
    val registry: DefaultIdRegistry[QueryGraph] = IdRegistry[QueryGraph]

    val nProp = prop("n", "prop")
    val mProp = prop("m", "prop")
    val joinPred = equals(nProp, mProp)
    val labelNPred = hasLabels("n", "N")
    val labelMPred = hasLabels("m", "M")
    new givenConfig() {
      indexOn("N", "prop")
      indexOn("M", "prop")
      addTypeToSemanticTable(nProp, CTAny)
      addTypeToSemanticTable(mProp, CTAny)
    }.withLogicalPlanningContext { (_, ctx) =>
      val order = InterestingOrderConfig.empty
      val kit = ctx.plannerState.config.toKit(order, ctx)
      val nQg = QueryGraph(patternNodes = Set(v"n")).addPredicates(labelNPred)
      val mQg = QueryGraph(
        patternNodes = Set(v"m"),
        optionalMatches = IndexedSeq(QueryGraph(patternNodes = Set(v"o")))
      ).addPredicates(labelMPred)
      val fullQg = (nQg ++ mQg).addPredicates(joinPred)

      val nPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "n")
      val mPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "m")
      ctx.staticComponents.planningAttributes.solveds.set(nPlan.id, RegularSinglePlannerQuery(nQg))
      ctx.staticComponents.planningAttributes.solveds.set(mPlan.id, RegularSinglePlannerQuery(mQg))
      table.put(register(registry, nQg), sorted = false, nPlan)
      table.put(register(registry, mQg), sorted = false, mPlan)
      val goal = register(registry, nQg, mQg)

      val step = NestedIndexJoinComponentConnector(singleComponentPlanner).solverStep(
        GoalBitAllocation(2, 0, Seq.empty),
        fullQg,
        order,
        kit,
        ctx
      )
      val plans = step(registry, goal, table, ctx).toSeq
      plans should contain theSameElementsAs Seq(
        Apply(
          mPlan,
          nodeIndexSeek(
            "n:N(prop = ???)",
            _ => CanGetValue,
            paramExpr = Some(mProp),
            argumentIds = Set("m"),
            labelId = 0
          )
        )
      )
    }
  }
}
