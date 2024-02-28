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

import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.UnPositionedVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.topDown

/**
 * Rewrites [[ForAllRepetitions]] predicates into expressions that the runtime can evaluate.
 */
case class ForAllRepetitionsPredicateRewriter(
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  solveds: Solveds,
  cardinalities: Cardinalities,
  providedOrders: ProvidedOrders,
  idGen: IdGen
) extends Rewriter {
  implicit val implicitIdGen: IdGen = idGen

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case far: ForAllRepetitions =>
      rewriteToAllIterablePredicate(far)
  })

  def rewriteToAllIterablePredicate(
    far: ForAllRepetitions
  ): Expression = {
    val pos = InputPosition.NONE
    val originalInnerPredicate = far.originalInnerPredicate

    val iterVar = UnPositionedVariable.varFor(anonymousVariableNameGenerator.nextName)

    val singletonReplacements: Set[VariableGrouping] =
      originalInnerPredicate.dependencies.flatMap(far.variableGroupingForSingleton)

    val singletonsToBeReplaced = singletonReplacements.map(_.singleton)

    val andedPropertyInequalitiesRewriter = topDown(Rewriter.lift {
      case AndedPropertyInequalities(variable, _, inequalities)
        if singletonsToBeReplaced.contains(variable) =>
        Ands.create(inequalities.toListSet)
    })

    val predicateWithoutAndedInequalities =
      originalInnerPredicate.endoRewrite(andedPropertyInequalitiesRewriter)

    val npeRewriter = topDown(Rewriter.lift {
      case nestedPlanExpression: NestedPlanExpression =>
        val singletonReplacements = nestedPlanExpression.dependencies.flatMap(far.variableGroupingForSingleton)
        val prependedPlan = prependGroupVariableProjection(
          originalInnerPredicate,
          iterVar,
          singletonReplacements,
          nestedPlanExpression.plan
        )

        nestedPlanExpression.withPlan(prependedPlan)
    })

    val otherRewriter = topDown(
      Rewriter.lift {
        case singletonVar: Variable if singletonsToBeReplaced.contains(singletonVar) =>
          val groupVar = singletonReplacements.find(_.singleton == singletonVar).get.group
          ContainerIndex(groupVar.copyId, iterVar.copyId)(pos)
      },
      stopper = _.isInstanceOf[NestedPlanExpression]
    )

    val rewrittenPredicate = predicateWithoutAndedInequalities.folder.treeFindByClass[NestedPlanExpression].map { _ =>
      predicateWithoutAndedInequalities
        .endoRewrite(npeRewriter)
        .endoRewrite(otherRewriter)
    }.getOrElse {
      singletonReplacements.foldLeft(predicateWithoutAndedInequalities) {
        case (expr, VariableGrouping(singletonVar, groupVar)) =>
          def indexedGroupVar: Expression = ContainerIndex(groupVar.copyId, iterVar.copyId)(pos)
          // x -> xGroup[iterVar]
          expr.replaceAllOccurrencesBy(singletonVar, indexedGroupVar)
      }
    }

    AllIterablePredicate(
      iterVar,
      functions.Range.asInvocation(
        SignedDecimalIntegerLiteral("0")(pos),
        Subtract(
          functions.Size(far.groupVariableAnchor.copyId)(pos),
          SignedDecimalIntegerLiteral("1")(pos)
        )(pos)
      )(pos),
      Some(rewrittenPredicate)
    )(originalInnerPredicate.position)
  }

  /**
   * Takes a `plan`, puts that on the RHS of an `Apply` and adds a projection of relevant variable groupings on the LHS:
   *
   * I.e., it turns
   * {{{
   *   .plan()
   * }}}
   *
   * into
   *
   * {{{
   *   .apply()
   *   .|.plan
   *   .projection("groupVar[iterVar] AS singletonVar")
   *   .argument()
   * }}}
   *
   * @param solvedPredicate the predicate that we want to solve
   * @param iterVar the index of the group variable to project
   * @param relevantVariableGroupings the variable groupings to project
   * @param plan the logical plan to hang on the RHS.
   **/
  private def prependGroupVariableProjection(
    solvedPredicate: Expression,
    iterVar: LogicalVariable,
    relevantVariableGroupings: Set[VariableGrouping],
    plan: LogicalPlan
  ): Apply = {
    // Argument
    val argumentIds = relevantVariableGroupings.map(_.group.copyId) + iterVar.copyId
    val argument = Argument(argumentIds)
    solveds.set(argument.id, solveds.get(plan.leftmostLeaf.id))
    cardinalities.set(argument.id, Cardinality.SINGLE)
    providedOrders.set(argument.id, ProvidedOrder.empty)

    // Projection
    val projectExpressions = relevantVariableGroupings.map {
      case VariableGrouping(singletonVar, groupVar) =>
        def indexedGroupVar: Expression = ContainerIndex(groupVar.copyId, iterVar.copyId)(InputPosition.NONE)

        singletonVar -> indexedGroupVar
    }.toMap
    val projection = Projection(argument, projectExpressions)
    val projectionSolveds = solveds.get(plan.leftmostLeaf.id).asSinglePlannerQuery.updateTailOrSelf(
      _.updateQueryProjection(_.withAddedProjections(projectExpressions))
    )
    solveds.set(projection.id, projectionSolveds)
    cardinalities.set(projection.id, Cardinality.SINGLE)
    providedOrders.set(projection.id, ProvidedOrder.empty)

    // Apply
    val connectingPlan = Apply(projection, plan)
    val previousTopPlanId = plan.id
    solveds.set(connectingPlan.id, solveds.get(previousTopPlanId))
    cardinalities.set(connectingPlan.id, cardinalities.get(previousTopPlanId))
    providedOrders.set(connectingPlan.id, providedOrders.get(previousTopPlanId))

    connectingPlan
  }

  override def apply(value: AnyRef): AnyRef = instance(value)
}
