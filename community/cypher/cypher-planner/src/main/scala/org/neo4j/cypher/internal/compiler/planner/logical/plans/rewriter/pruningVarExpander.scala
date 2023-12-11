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

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.VarExpandRewritePolicy.PreferDFS
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Length
import org.neo4j.cypher.internal.expressions.functions.Min
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
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
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable

/**
 * Before
 * EXPLAIN MATCH (n)-[r*]->(m) RETURN DISTINCT m
 * .produceResults(m)
 * .distinct(m)
 * .varExpand((n)-[r*]->(m))
 * .allNodeScan(n)
 *
 * After
 * EXPLAIN MATCH (n)-[r*]->(m) RETURN DISTINCT m
 * .produceResults(m)
 * .distinct(m)
 * .pruningVarExpand((n)-[r*]->(m))
 * .allNodeScan(n)
 *
 * Should run after [[TrailToVarExpandRewriter]] in order to rewrite as many VarExpand as possible.
 *
 * @param policy Determines whether a VarExpand(Into) should be planned as a BFS or DFS
 */
case class pruningVarExpander(
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  policy: VarExpandRewritePolicy
) extends Rewriter {

  sealed private trait HorizonPlan {
    def aggregationExpressions: Map[String, Expression]
  }

  private case class AggregatingHorizonPlan(aggregatingPlan: AggregatingPlan) extends HorizonPlan {

    override def aggregationExpressions: Map[String, Expression] =
      aggregatingPlan.aggregationExpressions.map { case (key, value) => key.name -> value }
  }

  private case object SemiApplyHorizonPlan extends HorizonPlan {
    override def aggregationExpressions: Map[String, Expression] = Map.empty
  }

  private case class DistinctHorizon(dependencies: Set[String], horizonPlan: HorizonPlan) {

    def withAddedDependencies(expressions: Iterable[Expression]): DistinctHorizon = {
      copy(dependencies = dependencies ++ expressions.flatMap(_.dependencies.map(_.name)))
    }

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
      !allDependenciesMinusMinPath(expand.relName.name)
    }

    /**
     * @return true if it is safe to rewrite this [[VarExpand]] to a [[PruningVarExpand]]
     */
    private def canReplaceWithPruning(expand: VarExpand): Boolean = {
      horizonPlan != null &&
      validMaxLength(expand, requireMaxLength = true) &&
      expand.mode == ExpandAll &&
      !allDependencies(expand.relName.name)
    }

    private def replaceMinPathLength(
      distanceName: String,
      expression: Expression,
      varExpand: VarExpand
    ): Option[Expression] = expression match {
      case minLength @ Min(length @ Length(PathExpression(step)))
        if step.dependencies.contains(varExpand.relName) =>
        Some(Min(Variable(distanceName)(length.position))(minLength.position))
      case minSize @ Min(size @ Size(variable: Variable)) if variable == varExpand.relName =>
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
      case Min(Length(PathExpression(step))) => isPathSafeToUse(step)
      case Min(Size(_: Variable))            => true
      case _                                 => false
    }

    /**
     * NOTE: It _should_ be possible to support cases where multiple relationships variables are used in the same path expression of min(length(path)),
     * e.g., by summing the 'distances' emitted by BFS like so: min(distance1 + distance2 + ...)
     * When that is implemented, this check can be removed.
     */
    private def isPathSafeToUse(step: PathStep): Boolean = step match {
      case NodePathStep(_, MultiRelationshipPathStep(_, _, _, _: NilPathStep)) => true
      case _                                                                   => false
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

  /**
   * @param pruningExpands set of [[VarExpand]] plans that can safely be rewritten to [[PruningVarExpand]].
   * @param bfsPruningExpands map of [[VarExpand]] plans that can safely be rewritten to [[BFSPruningVarExpand]] along with the variable name to write
   *                          the BFS distance to, if distance needs to be rewritten.
   * @param aggregatingPlans map of [[AggregatingPlan]] plans that contain grouping expressions which can be simplified/replaced,
   *                         along with the aggregation expressions to simplify.
   */
  private case class ReplacementPlans(
    pruningExpands: Set[Ref[VarExpand]],
    bfsPruningExpands: Map[Ref[VarExpand], Option[String]],
    aggregatingPlans: Map[Ref[AggregatingPlan], Map[String, Expression]]
  )

  private def findReplacementPlans(plan: LogicalPlan): ReplacementPlans = {
    val pruningExpands = mutable.Set[Ref[VarExpand]]()
    val bfsPruningExpands = mutable.Map[Ref[VarExpand], Option[String]]()
    val replacementAggregatingPlans = mutable.Map[Ref[AggregatingPlan], Map[String, Expression]]()

    /**
     * @return new LHS & RHS horizons
     */
    def collectDistinctSet(plan: LogicalPlan, distinctHorizon: DistinctHorizon): (DistinctHorizon, DistinctHorizon) = {
      plan match {
        case aggPlan: AggregatingPlan
          if aggPlan.aggregationExpressions.values.forall(e =>
            DistinctHorizon.isDistinct(e) || DistinctHorizon.isMinPathLength(e)
          ) =>
          val groupingDependencies = aggPlan.groupingExpressions.values.flatMap(_.dependencies.map(_.name)).toSet
          (DistinctHorizon(groupingDependencies, AggregatingHorizonPlan(aggPlan)), DistinctHorizon.empty)

        case expand: VarExpand if policy.accept(expand) =>
          distinctHorizon.getRewrite(expand) match {
            case RewriteToPruning =>
              pruningExpands += Ref(expand)
            case RewriteToBfs =>
              bfsPruningExpands.put(Ref(expand), None)
            case RewriteToBfsWithDepth(distanceName, newAggregationExpressions, aggregatingPlan) =>
              bfsPruningExpands.put(Ref(expand), Some(distanceName))
              replacementAggregatingPlans.updateWith(Ref(aggregatingPlan))({
                case Some(aggregationExpressions) => Some(aggregationExpressions ++ newAggregationExpressions)
                case None => Some(distinctHorizon.horizonPlan.aggregationExpressions ++ newAggregationExpressions)
              })
            case NoRewrite =>
          }
          val newHorizon = distinctHorizon
            .withAddedDependencies(expand.nodePredicates.map(_.predicate))
            .withAddedDependencies(expand.relationshipPredicates.map(_.predicate))
          (newHorizon, DistinctHorizon.empty)

        case Projection(_, expressions) if distinctHorizon.isInDistinctHorizon =>
          (distinctHorizon.withAddedDependencies(expressions.values), DistinctHorizon.empty)

        case Selection(Ands(predicates), _) if distinctHorizon.isInDistinctHorizon =>
          (distinctHorizon.withAddedDependencies(predicates), DistinctHorizon.empty)

        case optionalExpand: OptionalExpand =>
          (distinctHorizon.withAddedDependencies(optionalExpand.predicate), DistinctHorizon.empty)

        case _: Expand |
          _: Eager |
          _: Optional =>
          (distinctHorizon, DistinctHorizon.empty)

        /**
         * For Apply plans that _do_ introduce an argument, it is never safe to traverse both sides in the same horizon.
         * For example, the RHS is traversed first and may introduce a new horizon, which means the later LHS traversal will work in the
         * wrong (further downstream) horizon.
         *
         * Traverse RHS with same horizon.
         */
        case _: Apply =>
          (DistinctHorizon.empty, distinctHorizon)

        /**
         * For predicate-type binary plans that _do_ introduce an argument, it is never safe to traverse both sides in the same horizon.
         *
         * In theory, because the rows of RHS are never passed downstream, it _would_ be possible to traverse the LHS with same horizon,
         * but with current traversal logic it is impossible to know what dependencies will be introduced in the RHS.
         */
        case _: SemiApply |
          _: AntiSemiApply =>
          (DistinctHorizon.empty, DistinctHorizon(Set.empty, SemiApplyHorizonPlan))

        /**
         * For binary plans that do not introduce an argument, it is safe to traverse both sides in the same horizon, because it is impossible that
         * variables (e.g., relationships) introduced by a [[VarExpand]] on one side of the plan could be used on the other side.
         */
        case _: CartesianProduct |
          _: NodeHashJoin |
          _: LeftOuterHashJoin |
          _: RightOuterHashJoin |
          _: Union |
          _: ValueHashJoin =>
          val newHorizon = plan match {
            /**
             * [[ValueHashJoin]] needs its own case so dependencies from its join expression can be tracked.
             */
            case ValueHashJoin(_, _, org.neo4j.cypher.internal.expressions.Equals(lhs, rhs)) =>
              distinctHorizon.withAddedDependencies(Seq(lhs, rhs))
            case _ =>
              distinctHorizon
          }
          (newHorizon, newHorizon)

        case _ =>
          (DistinctHorizon.empty, DistinctHorizon.empty)
      }
    }

    val planStack = new mutable.Stack[(LogicalPlan, DistinctHorizon)]()
    planStack.push((plan, DistinctHorizon.empty))

    while (planStack.nonEmpty) {
      val (plan: LogicalPlan, distinctHorizon: DistinctHorizon) = planStack.pop()
      val (newLhsHorizon, newRhsHorizon) = collectDistinctSet(plan, distinctHorizon)
      plan.lhs.foreach(p => planStack.push((p, newLhsHorizon)))
      plan.rhs.foreach(p => planStack.push((p, newRhsHorizon)))
    }

    ReplacementPlans(pruningExpands.toSet, bfsPruningExpands.toMap, replacementAggregatingPlans.toMap)
  }

  override def apply(input: AnyRef): AnyRef = {
    input match {
      case plan: LogicalPlan =>
        val replacementPlans = findReplacementPlans(plan)

        val innerRewriter = topDown(
          Rewriter.lift {
            case expand @ VarExpand(
                lhs,
                fromId,
                dir,
                _,
                relTypes,
                toId,
                _,
                length,
                mode,
                nodePredicate,
                relationshipPredicate
              ) =>
              if (replacementPlans.bfsPruningExpands.contains(Ref(expand))) {
                BFSPruningVarExpand(
                  lhs,
                  fromId,
                  dir,
                  relTypes,
                  toId,
                  length.min == 0,
                  length.max.getOrElse(Int.MaxValue),
                  depthName = replacementPlans.bfsPruningExpands(Ref(expand)).map(varFor),
                  mode,
                  nodePredicate,
                  relationshipPredicate
                )(SameId(expand.id))
              } else if (replacementPlans.pruningExpands(Ref(expand))) {
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

            case aggregation: Aggregation if replacementPlans.aggregatingPlans.contains(Ref(aggregation)) =>
              aggregation.copy(aggregationExpressions = replacementPlans.aggregatingPlans(Ref(aggregation)).map {
                case (key, value) => varFor(key) -> value
              })(
                SameId(aggregation.id)
              )

            case aggregation: OrderedAggregation if replacementPlans.aggregatingPlans.contains(Ref(aggregation)) =>
              aggregation.copy(aggregationExpressions = replacementPlans.aggregatingPlans(Ref(aggregation)).map {
                case (key, value) => varFor(key) -> value
              })(
                SameId(aggregation.id)
              )
          },
          // We only rewrite Logical plans, like this we avoid traversing deeper into other objects
          stopper = !_.isInstanceOf[LogicalPlan]
        )
        plan.endoRewrite(innerRewriter)

      case _ => input
    }
  }
}

sealed trait VarExpandRewritePolicy {

  def accept(plan: VarExpand): Boolean =
    (this, plan.mode) match {
      case (PreferDFS, ExpandInto) => false
      case _                       => true
    }
}

object VarExpandRewritePolicy {

  /**
   * Indicates that a VarExpand(Into) should be left as a DFS, which is the default.
   */
  case object PreferDFS extends VarExpandRewritePolicy

  /**
   * Indicates that a VarExpand(Into) should be rewritten to a BFS where possible. This can have a negative impact on
   * performance in the worst case, so is not enabled by default.
   */
  case object PreferBFS extends VarExpandRewritePolicy

  val default: VarExpandRewritePolicy = PreferDFS
}
