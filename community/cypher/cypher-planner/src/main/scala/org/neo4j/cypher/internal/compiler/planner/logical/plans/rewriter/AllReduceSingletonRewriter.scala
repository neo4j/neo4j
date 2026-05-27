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

import org.neo4j.cypher.internal.expressions.AllReduceAccumulator
import org.neo4j.cypher.internal.expressions.AllReduceSingletonPredicate
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Repeat
import org.neo4j.cypher.internal.logical.plans.RepeatAcyclic
import org.neo4j.cypher.internal.logical.plans.RepeatTrail
import org.neo4j.cypher.internal.logical.plans.RepeatWalk
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.rewriting.conditions.ContainsNoMatchingPlanNodes
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * When inlining `AllReducePredicate`s, we generate `AllReduceSingletonPredicate`s on the RHS of a Repeat. These need to
 * be rewritten to a projection and a "normal" predicate instead and the aggregation and the Repeat needs to be updated
 * with the correct accumulator.
 *
 * {{{
 * .repeatTrail(`((left)-[rel]->(right))+ WITH sum = 0`)
 * .|.filter(isRepeatTrailUnique("rel"), AllReduceSingletonPredicate(sum, sum + rel.prop as sum, sum < 99))
 * }}}
 *
 * becomes
 *
 * {{{
 * .repeatTrail(`((left)-[rel]->(right))+ WITH sum = 0`)
 * .|.filter(isRepeatTrailUnique("rel"), "sum_1 < 99")
 * .|.projection("sum_0 + rel.prop AS sum_1")
 * }}}
 */
case class AllReduceSingletonRewriter(
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  solveds: Solveds,
  cardinalities: Cardinalities,
  providedOrders: ProvidedOrders,
  idGen: IdGen
) extends Rewriter {
  implicit val implicitIdGen: IdGen = idGen
  case class ProjectAndSelect(projection: (LogicalVariable, Expression), predicate: Expression)

  private def newRepeatParameters(rhs: LogicalPlan, accumulatorMappings: Set[AllReduceAccumulator]) = {
    val accumulatorsToNamespace =
      accumulatorMappings.collect {
        case AllReduceAccumulator(_, previous, next)
          if previous == next => previous
      }
    val renamings =
      accumulatorsToNamespace.map {
        (variable: LogicalVariable) =>
          variable -> varFor(Namespacer.genName(anonymousVariableNameGenerator, variableName = variable.name))
      }.toMap
    val newAccumulatorMappings =
      accumulatorMappings.map {
        case ara @ AllReduceAccumulator(_, previous, next) =>
          val newNext = renamings.getOrElse(previous, next)
          ara.copy(next = newNext)(ara.position)
      }
    val newRhs = rhs.endoRewrite(rhsRewriter(renamings))
    (newRhs, newAccumulatorMappings)
  }

  val innerRewriter: Rewriter = topDown(
    Rewriter.lift {
      case repeat: Repeat if repeat.accumulatorMappings.nonEmpty =>
        val (newRhs, newAccumulatorMappings) = newRepeatParameters(repeat.right, repeat.accumulatorMappings)
        repeat match {
          case typedRepeat: RepeatTrail =>
            typedRepeat.copy(right = newRhs, accumulatorMappings = newAccumulatorMappings)(SameId(repeat.id))
          case typedRepeat: RepeatWalk =>
            typedRepeat.copy(right = newRhs, accumulatorMappings = newAccumulatorMappings)(SameId(repeat.id))
          case typedRepeat: RepeatAcyclic =>
            typedRepeat.copy(right = newRhs, accumulatorMappings = newAccumulatorMappings)(SameId(repeat.id))
        }
    },
    stopper = !_.isInstanceOf[LogicalPlan]
  )

  private def rhsRewriter(renamings: Map[LogicalVariable, LogicalVariable]) = topDown(
    Rewriter.lift {
      case sel @ Selection(predicates, source) =>
        val projectAndSelects =
          predicates.exprs.collect {
            case arsp @ AllReduceSingletonPredicate(previousAcc, reductionStep, predicate) =>
              val nextAccumulator =
                // if we still have a AllReduceSingletPredicate, then the accumulators in Repeat should be equal,
                // and we should have created a renamings entry.
                renamings(previousAcc)
              arsp -> ProjectAndSelect(
                projection = (nextAccumulator, reductionStep),
                predicate = predicate.replaceAllOccurrencesBy(previousAcc, nextAccumulator)
              )
          }
            .toMap
        if (projectAndSelects.isEmpty) {
          sel
        } else {
          val projections = projectAndSelects.values.map(_.projection).toMap

          // We need to project the incoming accumulator to the outgoing accumulator using the reduction step.
          // In the RHS of a QPP there should not be a projection yet. Therefore, we do not need to incorporate any
          // logic here to join them. We might want to reconsider, if that should change.
          val projection = Projection(source, projections)
          cardinalities.set(projection.id, cardinalities.get(source.id))
          providedOrders.set(projection.id, providedOrders.get(source.id))
          solveds.set(
            projection.id,
            solveds.get(source.id)
              .asSinglePlannerQuery
              .updateTailOrSelf(
                _.updateQueryProjection(_.withAddedProjections(projections))
              )
          )

          val newPredicates =
            predicates.exprs.map {
              case arsp: AllReduceSingletonPredicate => projectAndSelects(arsp).predicate
              case otherPred                         => otherPred
            }
          sel.copy(
            predicate = Ands(newPredicates)(predicates.position),
            source = projection
          )(SameId(sel.id))
        }
    },
    stopper = !_.isInstanceOf[LogicalPlan]
  )

  override def apply(input: AnyRef): AnyRef = innerRewriter(input)
}

case object NoAllReducePredicatesLeft extends ContainsNoMatchingPlanNodes {

  override val matcher: PartialFunction[ASTNode, String] = {
    case _: AllReduceSingletonPredicate => "AllReduceSingletonPredicate(...)"
  }
  override val name: String = "NoAllReducePredicatesLeft"

}
