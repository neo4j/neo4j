/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Length
import org.neo4j.cypher.internal.expressions.functions.Min
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalUnaryPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable

case class pruningVarExpander(anonymousVariableNameGenerator: AnonymousVariableNameGenerator) extends Rewriter {

  sealed private trait HorizonPlan {
    def aggregationExpressions: Map[String, Expression]
  }

  private case class AggregatingHorizonPlan(aggregatingPlan: AggregatingPlan) extends HorizonPlan {
    override def aggregationExpressions: Map[String, Expression] = aggregatingPlan.aggregationExpressions
  }

  private case object SemiApplyHorizonPlan extends HorizonPlan {
    override def aggregationExpressions: Map[String, Expression] = Map.empty
  }

  private case class DistinctHorizon(dependencies: Set[String], horizonPlan: HorizonPlan) {

    private lazy val (
      allDependenciesMinusMinPath: Set[String],
      allDependencies: Set[String],
      minPathExpressions: Map[String, Expression]
    ) = {
      if (horizonPlan == null) {
        (null, null, null)
      } else {
        val (_minPath, _rest) =
          horizonPlan.aggregationExpressions.partition(x => DistinctHorizon.isMinPathLength(x._2))
        val _aggregatingDependenciesMinusMinPath = _rest.values.flatMap(_.dependencies.map(_.name)).toSet
        (
          dependencies ++ _aggregatingDependenciesMinusMinPath,
          dependencies ++ horizonPlan.aggregationExpressions.values.flatMap(_.dependencies.map(_.name)).toSet,
          _minPath
        )
      }
    }

    def isInDistinctHorizon: Boolean = horizonPlan != null

    def getRewrite(expand: VarExpand): VarExpandRewrite = {
      if (canReplaceWithBfsPruning(expand)) {

        /**
         * [[BFSPruningVarExpand]] can also emit (shortest path) depth with every node, which makes it possible to rewrite certain [[Aggregation]] plans.
         *
         * For example,
         * MATCH path=(a)-[*1..2]-(b) RETURN min(length(path))
         * Can be rewritten to,
         * MATCH (a)-[*1..2]-(b) RETURN min(depth)
         * Where 'depth' is emitted by [[BFSPruningVarExpand]].
         */
        val rewrittenMinPathExpressions: mutable.Map[String, Expression] = mutable.Map.empty
        val distanceName = anonymousVariableNameGenerator.nextName
        minPathExpressions.foreach { case (key, value) =>
          replaceMinPathLength(distanceName, value, expand) match {
            case Some(rewrittenExpression) =>
              rewrittenMinPathExpressions.put(key, rewrittenExpression)
            case None =>
            // do nothing
          }
        }

        horizonPlan match {
          case AggregatingHorizonPlan(aggregatingPlan) if rewrittenMinPathExpressions.nonEmpty =>
            RewriteToBfsWithDepth(distanceName, rewrittenMinPathExpressions.toMap, aggregatingPlan)
          case _ =>
            RewriteToBfs
        }
      } else if (canReplaceWithPruning(expand)) {
        RewriteToPruning
      } else {
        NoRewrite
      }
    }

    /**
     * @return true if it is safe to rewrite this [[VarExpand]] to a [[BFSPruningVarExpand]]
     */
    private def canReplaceWithBfsPruning(expand: VarExpand): Boolean = {
      horizonPlan != null &&
      expand.length.min <= 1 &&
      validMaxLength(expand, requireMaxLength = false) &&
      !allDependenciesMinusMinPath(expand.relName)
    }

    /**
     * @return true if it is safe to rewrite this [[VarExpand]] to a [[PruningVarExpand]]
     */
    private def canReplaceWithPruning(expand: VarExpand): Boolean = {
      horizonPlan != null &&
      validMaxLength(expand, requireMaxLength = true) &&
      !allDependencies(expand.relName)
    }

    private def replaceMinPathLength(
      distanceName: String,
      expression: Expression,
      varExpand: VarExpand
    ): Option[Expression] = expression match {
      case minLength @ Min(length @ Length(PathExpression(step)))
        if step.dependencies.map(_.name).contains(varExpand.relName) =>
        Some(Min(Variable(distanceName)(length.position))(minLength.position))
      case minSize @ Min(size @ Size(variable: Variable)) if variable.name == varExpand.relName =>
        Some(Min(Variable(distanceName)(size.position))(minSize.position))
      case _ =>
        None
    }

    private def validMaxLength(expand: VarExpand, requireMaxLength: Boolean): Boolean = expand.length.max match {
      case Some(max) => max > 1 && expand.length.min <= max
      case _         => !requireMaxLength
    }
  }

  private object DistinctHorizon {
    val empty: DistinctHorizon = DistinctHorizon(Set.empty, null)

    def isDistinct(e: Expression): Boolean = e match {
      case f: FunctionInvocation => f.distinct
      case _                     => false
    }

    def isMinPathLength(e: Expression): Boolean = e match {
      case Min(Length(_: PathExpression)) => true
      case Min(Size(_: Variable))         => true
      case _                              => false
    }
  }

  sealed trait VarExpandRewrite

  case object RewriteToPruning extends VarExpandRewrite

  case object RewriteToBfs extends VarExpandRewrite

  case class RewriteToBfsWithDepth(
    distanceName: String,
    newAggregationExpressions: Map[String, Expression],
    aggregatingPlan: AggregatingPlan
  ) extends VarExpandRewrite

  case object NoRewrite extends VarExpandRewrite

  private case class ReplacementPlans(
    pruningExpands: Set[VarExpand],
    bfsPruningExpands: Map[VarExpand, Option[String]],
    aggregatingPlans: Map[AggregatingPlan, Map[String, Expression]]
  )

  private def findReplacementPlans(plan: LogicalPlan): ReplacementPlans = {
    val pruningExpands = mutable.Set[VarExpand]()
    val bfsPruningExpands = mutable.Map[VarExpand, Option[String]]()
    val replacementAggregatingPlans = mutable.Map[AggregatingPlan, Map[String, Expression]]()

    def collectDistinctSet(plan: LogicalPlan, distinctHorizon: DistinctHorizon): DistinctHorizon = {
      plan match {
        case aggPlan: AggregatingPlan
          if aggPlan.aggregationExpressions.values.forall(e =>
            DistinctHorizon.isDistinct(e) || DistinctHorizon.isMinPathLength(e)
          ) =>
          val groupingDependencies = aggPlan.groupingExpressions.values.flatMap(_.dependencies.map(_.name)).toSet
          DistinctHorizon(groupingDependencies, AggregatingHorizonPlan(aggPlan))

        case expand: VarExpand =>
          distinctHorizon.getRewrite(expand) match {
            case RewriteToPruning =>
              pruningExpands += expand
              distinctHorizon
            case RewriteToBfs =>
              bfsPruningExpands.put(expand, None)
              distinctHorizon
            case RewriteToBfsWithDepth(distanceName, newAggregationExpressions, aggregatingPlan) =>
              bfsPruningExpands.put(expand, Some(distanceName))
              replacementAggregatingPlans.updateWith(aggregatingPlan)({
                case Some(aggregationExpressions) => Some(aggregationExpressions ++ newAggregationExpressions)
                case None => Some(distinctHorizon.horizonPlan.aggregationExpressions ++ newAggregationExpressions)
              })
              distinctHorizon
            case NoRewrite =>
              distinctHorizon
          }

        case Projection(_, _, expressions) if distinctHorizon.isInDistinctHorizon =>
          distinctHorizon.copy(dependencies =
            distinctHorizon.dependencies ++ expressions.values.flatMap(_.dependencies.map(_.name))
          )

        case Selection(Ands(predicates), _) if distinctHorizon.isInDistinctHorizon =>
          distinctHorizon.copy(dependencies =
            distinctHorizon.dependencies ++ predicates.flatMap(_.dependencies.map(_.name))
          )

        case _: Expand |
          _: Apply |
          _: SemiApply |
          _: AntiSemiApply |
          _: CartesianProduct |
          _: Eager |
          _: Optional |
          _: OptionalExpand |
          _: NodeHashJoin |
          _: LeftOuterHashJoin |
          _: RightOuterHashJoin |
          _: Union =>
          distinctHorizon

        /**
         * [[ValueHashJoin]] needs its own case so dependencies from its join expression can be tracked.
         */
        case ValueHashJoin(_, _, org.neo4j.cypher.internal.expressions.Equals(lhs, rhs)) =>
          distinctHorizon.copy(dependencies =
            distinctHorizon.dependencies ++ Seq(lhs, rhs).flatMap(_.dependencies.map(_.name))
          )

        case _ =>
          DistinctHorizon.empty
      }
    }

    val planStack = new mutable.Stack[(LogicalPlan, DistinctHorizon)]()
    planStack.push((plan, DistinctHorizon.empty))

    while (planStack.nonEmpty) {
      val (plan: LogicalPlan, distinctHorizon: DistinctHorizon) = planStack.pop()
      val newDistinctHorizon = collectDistinctSet(plan, distinctHorizon)

      val (lhsHorizon, rhsHorizon) = plan match {
        case _: CartesianProduct |
          _: Union |
          _: NodeHashJoin |
          _: LeftOuterHashJoin |
          _: RightOuterHashJoin |
          _: ValueHashJoin =>
          /**
           * For binary plans that do not introduce an argument, it is safe to traverse both sides in the same horizon, because it is impossible that
           * variables (e.g., relationships) introduced by a [[VarExpand]] on one side of the plan could be used on the other side.
           * An exception to this rule is if those variables are used in the predicate of [[ValueHashJoin]], but these dependencies are tracked elsewhere.
           */
          (newDistinctHorizon, newDistinctHorizon)
        case apply: AbstractSemiApply =>
          /**
           * For predicate-type binary plans that _do_ introduce an argument, it is never safe to traverse both sides in the same horizon.
           * Because the rows of RHS are never passed downstream, the LHS is traversed with same horizon.
           */
          (newDistinctHorizon, DistinctHorizon(Set.empty, SemiApplyHorizonPlan))
        case _: Apply =>
          /**
           * For Apply plans that _do_ introduce an argument, it is never safe to traverse both sides in the same horizon.
           * For example, the RHS is traversed first and may introduce a new horizon, which means the later LHS traversal will work in the
           * wrong (further downstream) horizon.
           *
           * Traverse RHS with same horizon.
           */
          (DistinctHorizon.empty, newDistinctHorizon)
        case _: LogicalUnaryPlan =>
          (newDistinctHorizon, DistinctHorizon.empty)
        case _ =>
          (DistinctHorizon.empty, DistinctHorizon.empty)
      }
      plan.lhs.foreach(p => planStack.push((p, lhsHorizon)))
      plan.rhs.foreach(p => planStack.push((p, rhsHorizon)))
    }

    ReplacementPlans(pruningExpands.toSet, bfsPruningExpands.toMap, replacementAggregatingPlans.toMap)
  }

  override def apply(input: AnyRef): AnyRef = {
    input match {
      case plan: LogicalPlan =>
        val replacementPlans = findReplacementPlans(plan)

        val innerRewriter = topDown(Rewriter.lift {
          case expand @ VarExpand(
              lhs,
              fromId,
              dir,
              _,
              relTypes,
              toId,
              _,
              length,
              ExpandAll,
              nodePredicate,
              relationshipPredicate
            ) =>
            if (replacementPlans.bfsPruningExpands.contains(expand)) {
              BFSPruningVarExpand(
                lhs,
                fromId,
                dir,
                relTypes,
                toId,
                length.min == 0,
                length.max.getOrElse(Int.MaxValue),
                depthName = replacementPlans.bfsPruningExpands(expand),
                nodePredicate,
                relationshipPredicate
              )(SameId(expand.id))
            } else if (replacementPlans.pruningExpands(expand)) {
              PruningVarExpand(
                lhs,
                fromId,
                dir,
                relTypes,
                toId,
                length.min,
                length.max.get,
                nodePredicate,
                relationshipPredicate
              )(SameId(expand.id))
            } else {
              expand
            }

          case aggregation: Aggregation if replacementPlans.aggregatingPlans.contains(aggregation) =>
            aggregation.copy(aggregationExpressions = replacementPlans.aggregatingPlans(aggregation))(
              SameId(aggregation.id)
            )

          case aggregation: OrderedAggregation if replacementPlans.aggregatingPlans.contains(aggregation) =>
            aggregation.copy(aggregationExpressions = replacementPlans.aggregatingPlans(aggregation))(
              SameId(aggregation.id)
            )
        })
        plan.endoRewrite(innerRewriter)

      case _ => input
    }
  }
}
