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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.LogicalPlanRewritten
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AndsReorderable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Selection.LabelAndRelTypeInfo
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.PredicateCost
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

import scala.collection.immutable.ListSet

/**
 * Sorts the predicates in [[Selection]] plans according to their selectivity and cost. Tests can be found in
 * SelectionPlanningIntegrationTest.
 */
case object SortPredicatesBySelectivity extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState]
    with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING

  override def preConditions: Set[StepSequencer.Condition] = Set(LogicalPlanRewritten)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    val rewriter = {
      bottomUp(Rewriter.lift {
        case s: Selection if s.predicate.exprs.size > 1 =>
          val newPredicates = sortSelectionPredicates(from, context, s)
          s.copy(predicate = Ands(newPredicates)(s.predicate.position))(SameId(s.id))
      })
    }
    val plan = from.logicalPlan.endoRewrite(rewriter)
    from.withMaybeLogicalPlan(Some(plan))
  }

  private def sortSelectionPredicates(
    from: LogicalPlanState,
    context: PlannerContext,
    s: Selection
  ): Seq[Expression] = {
    val LabelAndRelTypeInfo(labelInfo, relTypeInfo) = from.planningAttributes.labelAndRelTypeInfos.get(s.id) match {
      case Some(value) => value
      case None =>
        AssertMacros.checkOnlyWhenAssertionsAreEnabled(
          false,
          s"labelAndRelTypeInfos should always be defined on selections. Selection plan id: ${s.id.x}"
        )
        LabelAndRelTypeInfo(Map.empty, Map.empty)
    }

    if (s.predicate.exprs.size < 2) {
      s.predicate.exprs.toSeq
    } else {
      val incomingCardinality = from.planningAttributes.cardinalities.get(s.source.id)
      val solvedBeforePredicate = from.planningAttributes.solveds.get(s.source.id) match {
        case query: SinglePlannerQuery => query
        case _: UnionQuery             =>
          // In case we re-order predicates after the plan has already been rewritten,
          // there is a chance that the source plan solves a UNION query.
          // In that case we just pretend it solves an Argument with the same available symbols.
          RegularSinglePlannerQuery(QueryGraph(argumentIds = s.source.availableSymbols))
      }

      def sortCriteria(predicate: Expression): PredicateCost = {
        val costPerRow = CardinalityCostModel.costPerRowFor(predicate, from.semanticTable())
        val solved = solvedBeforePredicate.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicate)))
        val cardinality = context.metrics.cardinality(
          solved,
          labelInfo,
          relTypeInfo,
          from.semanticTable(),
          IndexCompatiblePredicatesProviderContext.default
        )
        val selectivity = (cardinality / incomingCardinality).getOrElse(Selectivity.ONE)
        PredicateCost(costPerRow, selectivity)
      }

      val sortedPredicates = s.predicate.exprs.toSeq
        .map(p => (p, sortCriteria(p)))
        .sortBy(_._2)

      // gather consecutive predicates with the same cost
      val groupedByCost: Seq[Seq[Expression]] =
        sortedPredicates.foldLeft(List.empty[(PredicateCost, Seq[Expression])]) {
          case (groups, (expr, exprCost)) =>
            groups match {
              case (groupCost, exprs) :: groupsTail if groupCost.equalsWithTolerance(exprCost) =>
                (groupCost, (exprs :+ expr)) :: groupsTail
              case _ =>
                (exprCost, Seq(expr)) :: groups
            }
        }.map(_._2).reverse

      /** ungroup predicates without store access and wrap remaining grouped predicates into [[AndsReorderable]] */
      groupedByCost
        .flatMap { group =>
          val (hasStoreAccess, noStoreAccess) =
            group.partition(expr => CardinalityCostModel.calculateNumberOfStoreAccesses(expr, from.semanticTable()) > 0)
          noStoreAccess.map(e => Seq(e)) :+ hasStoreAccess
        }
        .flatMap { exprs =>
          if (exprs.size == 1) exprs
          else if (exprs.size > 1) Seq(AndsReorderable(ListSet.from(exprs))(exprs.head.position))
          else Seq.empty
        }
    }
  }

}
