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
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ValueMergeJoin
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder

class ValueMergeJoinComponentConnectorTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport2 {

  private def newGivenLogicalPlanningContextContext(): LogicalPlanningContext = {
    new givenConfig().withLogicalPlanningContext { (_, ctx) =>
      ctx.withModifiedSettings(_.copy(planningMergeJoinEnabled = true))
    }
  }

  private def solverStep(fullQg: QueryGraph, context: LogicalPlanningContext): ComponentConnectorSolverStep = {
    val (gba, _) = GoalBitAllocation.create(fullQg.connectedComponents.toSet, fullQg.optionalMatches)
    val ioc = InterestingOrderConfig.empty
    val kit = context.plannerState.config.toKit(ioc, context)
    ValueMergeJoinComponentConnector.solverStep(gba, fullQg, ioc, kit, context)
  }

  private def fakePlanForQg(
    qg: QueryGraph,
    po: ProvidedOrder,
    context: LogicalPlanningContext,
    extraAvailableSymbols: String*
  ): LogicalPlan = {
    val attributes = context.staticComponents.planningAttributes
    val plan = fakeLogicalPlanFor(attributes, (extraAvailableSymbols ++ qg.patternNodes.toSeq.map(_.name)): _*)
    attributes.providedOrders.set(plan.id, po)
    plan
  }

  private case class IdpTableEntry(
    qgs: Seq[QueryGraph],
    plan: LogicalPlan,
    sorted: Boolean,
    hasExtraProperties: Boolean
  )

  private def buildIdpState(idpTableEntries: IdpTableEntry*): (IDPTable[LogicalPlan], IdRegistry[QueryGraph], Goal) = {
    val table = IDPTable.empty[LogicalPlan]
    val registry = IdRegistry[QueryGraph]

    for (entry <- idpTableEntries) {
      table.put(
        Goal(registry.registerAll(entry.qgs)),
        sorted = entry.sorted,
        hasExtraProperties = entry.hasExtraProperties,
        entry.plan
      )
    }
    (table, registry, Goal(registry.registerAll(idpTableEntries.flatMap(_.qgs))))
  }

  private def ASC(expr: Expression): ProvidedOrder = DefaultProvidedOrderFactory.asc(expr)
  private def DESC(expr: Expression): ProvidedOrder = DefaultProvidedOrderFactory.desc(expr)

  test("produces merge join of two components connected by property equality ASC") {
    // given
    val context = newGivenLogicalPlanningContextContext()

    val nQg = QueryGraph(patternNodes = Set(v"n"))
    val mQg = QueryGraph(patternNodes = Set(v"m"))
    val joinPred = equals(prop("n", "prop"), prop("m", "prop"))

    val fullQg = (nQg ++ mQg).addPredicates(joinPred)

    val nPlan = fakePlanForQg(nQg, ASC(joinPred.lhs), context)
    val mPlan = fakePlanForQg(mQg, ASC(joinPred.rhs), context)

    val (table, registry, goal) = buildIdpState(
      IdpTableEntry(Seq(nQg), nPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(mQg), mPlan, sorted = false, hasExtraProperties = false)
    )

    // then
    val step = solverStep(fullQg, context)
    val plans = step(registry, goal, table, context).toSeq
    plans should contain theSameElementsAs Seq(
      ValueMergeJoin(nPlan, mPlan, joinPred),
      ValueMergeJoin(mPlan, nPlan, joinPred.switchSides)
    )
  }

  test("produces merge join of two components connected by property equality DESC") {
    // given
    val context = newGivenLogicalPlanningContextContext()

    val nQg = QueryGraph(patternNodes = Set(v"n"))
    val mQg = QueryGraph(patternNodes = Set(v"m"))
    val joinPred = equals(prop("n", "prop"), prop("m", "prop"))

    val fullQg = (nQg ++ mQg).addPredicates(joinPred)

    val nPlan = fakePlanForQg(nQg, DESC(joinPred.lhs), context)
    val mPlan = fakePlanForQg(mQg, DESC(joinPred.rhs), context)

    val (table, registry, goal) = buildIdpState(
      IdpTableEntry(Seq(nQg), nPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(mQg), mPlan, sorted = false, hasExtraProperties = false)
    )

    // then
    val step = solverStep(fullQg, context)
    val plans = step(registry, goal, table, context).toSeq
    plans should contain theSameElementsAs Seq(
      ValueMergeJoin(nPlan, mPlan, joinPred),
      ValueMergeJoin(mPlan, nPlan, joinPred.switchSides)
    )
  }

  test("produces no merge join of two components without a predicate") {
    // given
    val context = newGivenLogicalPlanningContextContext()

    val nQg = QueryGraph(patternNodes = Set(v"n"))
    val mQg = QueryGraph(patternNodes = Set(v"m"))
    val fullQg = nQg ++ mQg

    val nPlan = fakePlanForQg(nQg, ASC(prop("n", "prop")), context)
    val mPlan = fakePlanForQg(mQg, ASC(prop("m", "prop")), context)

    val (table, registry, goal) = buildIdpState(
      IdpTableEntry(Seq(nQg), nPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(mQg), mPlan, sorted = false, hasExtraProperties = false)
    )

    // then
    val step = solverStep(fullQg, context)
    val plans = step(registry, goal, table, context).toSeq
    plans should be(empty)
  }

  test("produces no merge join of two components connected by property equality, only one ordered") {
    // given
    val context = newGivenLogicalPlanningContextContext()

    val nQg = QueryGraph(patternNodes = Set(v"n"))
    val mQg = QueryGraph(patternNodes = Set(v"m"))
    val joinPred = equals(prop("n", "prop"), prop("m", "prop"))

    val fullQg = (nQg ++ mQg).addPredicates(joinPred)

    val nPlan = fakePlanForQg(nQg, ASC(joinPred.lhs), context)
    val mPlan = fakePlanForQg(mQg, ProvidedOrder.empty, context)

    val (table, registry, goal) = buildIdpState(
      IdpTableEntry(Seq(nQg), nPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(mQg), mPlan, sorted = false, hasExtraProperties = false)
    )

    // then
    val step = solverStep(fullQg, context)
    val plans = step(registry, goal, table, context).toSeq
    plans should be(empty)
  }

  test("produces no merge join of two components connected by property equality, different order direction") {
    // given
    val context = newGivenLogicalPlanningContextContext()

    val nQg = QueryGraph(patternNodes = Set(v"n"))
    val mQg = QueryGraph(patternNodes = Set(v"m"))
    val joinPred = equals(prop("n", "prop"), prop("m", "prop"))

    val fullQg = (nQg ++ mQg).addPredicates(joinPred)

    val nPlan = fakePlanForQg(nQg, ASC(joinPred.lhs), context)
    val mPlan = fakePlanForQg(mQg, DESC(joinPred.rhs), context)

    val (table, registry, goal) = buildIdpState(
      IdpTableEntry(Seq(nQg), nPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(mQg), mPlan, sorted = false, hasExtraProperties = false)
    )

    // then
    val step = solverStep(fullQg, context)
    val plans = step(registry, goal, table, context).toSeq
    plans should be(empty)
  }

  test("produces merge join plans from sorted plans and plans with extra properties") {

    def allPlanVariantsForQg(qg: QueryGraph, expr: Expression, context: LogicalPlanningContext): Seq[IdpTableEntry] = {
      for {
        (sorted, hasProperties) <- Seq((false, false), (true, false), (false, true))
      } yield {
        // makes all plans different
        val extraSymbol = {
          if (sorted) "sorted"
          else if (hasProperties) "has-properties"
          else "none"
        }
        IdpTableEntry(
          qgs = Seq(qg),
          plan = fakePlanForQg(qg, ASC(expr), context, extraSymbol),
          sorted = sorted,
          hasExtraProperties = hasProperties
        )
      }
    }

    // given
    val context = newGivenLogicalPlanningContextContext()

    val nQg = QueryGraph(patternNodes = Set(v"n"))
    val mQg = QueryGraph(patternNodes = Set(v"m"))
    val joinPred = equals(prop("n", "prop"), prop("m", "prop"))

    val fullQg = (nQg ++ mQg).addPredicates(joinPred)

    val nIdpEntries = allPlanVariantsForQg(nQg, joinPred.lhs, context)
    val mIdpEntries = allPlanVariantsForQg(mQg, joinPred.rhs, context)
    val (table, registry, goal) = buildIdpState(nIdpEntries ++ mIdpEntries: _*)

    // then
    val expectedPlans = for {
      nPlan <- nIdpEntries.map(_.plan)
      mPlan <- mIdpEntries.map(_.plan)
      mergePlan <- Seq(
        ValueMergeJoin(nPlan, mPlan, joinPred),
        ValueMergeJoin(mPlan, nPlan, joinPred.switchSides)
      )
    } yield {
      mergePlan
    }

    val step = solverStep(fullQg, context)
    val plans = step(registry, goal, table, context).toSeq
    plans.size shouldBe (3 * 3) * 2
    plans should contain theSameElementsAs expectedPlans
  }

  test("produces merge join of three components") {
    // given
    val context = newGivenLogicalPlanningContextContext()

    val nProp = prop("n", "prop")
    val mProp = prop("m", "prop")
    val oProp = prop("o", "prop")

    val nQg = QueryGraph(patternNodes = Set(v"n"))
    val mQg = QueryGraph(patternNodes = Set(v"m"))
    val oQg = QueryGraph(patternNodes = Set(v"o"))
    val nmJoin = equals(nProp, mProp)
    val moJoin = equals(mProp, oProp)
    val onJoin = equals(oProp, nProp)

    val fullQg = (nQg ++ mQg ++ oQg).addPredicates(nmJoin, moJoin, onJoin)

    val nPlan = fakePlanForQg(nQg, ASC(nProp), context)
    val mPlan = fakePlanForQg(mQg, ASC(mProp), context)
    val oPlan = fakePlanForQg(oQg, ASC(oProp), context)
    val nmPlan = fakePlanForQg((nQg ++ mQg), ASC(nProp), context)
    val moPlan = fakePlanForQg((mQg ++ oQg), ASC(mProp), context)
    val onPlan = fakePlanForQg((oQg ++ nQg), ASC(oProp), context)

    val (table, registry, goal) = buildIdpState(
      IdpTableEntry(Seq(nQg), nPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(mQg), mPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(oQg), oPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(nQg, mQg), nmPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(mQg, oQg), moPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(oQg, nQg), onPlan, sorted = false, hasExtraProperties = false)
    )

    // then
    val step = solverStep(fullQg, context)
    val plans = step(registry, goal, table, context).toSeq
    plans should contain theSameElementsAs Seq(
      ValueMergeJoin(nPlan, moPlan, nmJoin),
      ValueMergeJoin(moPlan, nPlan, nmJoin.switchSides),
      ValueMergeJoin(mPlan, onPlan, moJoin),
      ValueMergeJoin(onPlan, mPlan, moJoin.switchSides),
      ValueMergeJoin(oPlan, nmPlan, onJoin),
      ValueMergeJoin(nmPlan, oPlan, onJoin.switchSides)
    )
  }

  test("produces no plans if feature flag is disabled") {
    // given
    val context = newGivenLogicalPlanningContextContext()
      .withModifiedSettings(_.copy(planningMergeJoinEnabled = false))

    val nQg = QueryGraph(patternNodes = Set(v"n"))
    val mQg = QueryGraph(patternNodes = Set(v"m"))
    val joinPred = equals(prop("n", "prop"), prop("m", "prop"))

    val fullQg = (nQg ++ mQg).addPredicates(joinPred)

    val nPlan = fakePlanForQg(nQg, ASC(joinPred.lhs), context)
    val mPlan = fakePlanForQg(mQg, ASC(joinPred.rhs), context)

    val (table, registry, goal) = buildIdpState(
      IdpTableEntry(Seq(nQg), nPlan, sorted = false, hasExtraProperties = false),
      IdpTableEntry(Seq(mQg), mPlan, sorted = false, hasExtraProperties = false)
    )

    // then
    val step = solverStep(fullQg, context)
    val plans = step(registry, goal, table, context).toSeq
    plans should be(empty)
  }
}
