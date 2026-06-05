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

import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode.DisjointByAuto
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode.DisjointByExpressions
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode.DisjointByNone
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByParameters
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.FusedMerge
import org.neo4j.cypher.internal.logical.plans.LockNodes
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MergeInto
import org.neo4j.cypher.internal.logical.plans.MergeUniqueNode
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.SetDynamicProperty
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetProperties
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperties
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency.Concurrent
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenBU
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.TopDownMergeableRewriter
import org.neo4j.cypher.internal.util.RewriterStopper
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.topDown

case class TransactionDisjointByRewriter(globalStrategyIsAuto: Boolean) extends Rewriter
    with TopDownMergeableRewriter {

  override def apply(input: AnyRef): AnyRef = {
    instance.apply(input)
  }

  override val innerRewriter: Rewriter = Rewriter.lift {
    case t @ TransactionApply(lhs, rhs, _, Concurrent(_), _, _, _, maybeDisjointByParameters, _)
      if shouldInfer(maybeDisjointByParameters) =>
      val inputVars = lhs.availableSymbols
      val acc = findRaids(rhs, inputVars)
      acc match {
        case Acc(raids, _, Allowed) if raids.nonEmpty =>
          t.copy(effectiveDisjointBy = raids.toSeq)(SameId(t.id))
        case _ =>
          t
      }

    case t @ TransactionForeach(lhs, rhs, _, Concurrent(_), _, _, _, maybeDisjointByParameters, _)
      if shouldInfer(maybeDisjointByParameters) =>
      val inputVars = lhs.availableSymbols
      val acc = findRaids(rhs, inputVars)
      acc match {
        case Acc(raids, _, Allowed) if raids.nonEmpty =>
          t.copy(effectiveDisjointBy = raids.toSeq)(SameId(t.id))
        case _ =>
          t
      }
  }

  private def shouldInfer(maybeDisjointByParameters: Option[InTransactionsDisjointByParameters]): Boolean =
    maybeDisjointByParameters match {
      case Some(InTransactionsDisjointByParameters(DisjointByAuto))           => true
      case Some(InTransactionsDisjointByParameters(DisjointByNone))           => false
      case Some(InTransactionsDisjointByParameters(DisjointByExpressions(_))) => false
      case None                                                               => globalStrategyIsAuto
    }

  // TransactionApply/TransactionForeach is only allowed to occur at the top-level, never nested inside
  // expressions, so there is no need to descend into anything that is not a LogicalPlan.
  private val stopper: RewriterStopper = !_.isInstanceOf[LogicalPlan]

  private val instance: Rewriter = topDown(innerRewriter, stopper = stopper)

  private def findRaids(plan: LogicalPlan, inputVars: Set[LogicalVariable]): Acc = {
    val result = plan.folder.treeFoldBottomUp(Acc.empty) {
      // Only exact seeks uniquely identify a locked entity by value.
      case nuis: NodeUniqueIndexSeek if nuis.valueExpr.exact && nuis.valueExpr.expressions.nonEmpty =>
        TraverseChildrenBU { acc =>
          val usedVariables = nuis.usedVariables
          if (usedVariables.subsetOf(inputVars)) {
            val exprs = nuis.valueExpr.expressions
            acc.copy(
              raidExpressions = acc.raidExpressions ++ exprs,
              raidEntityVars = acc.raidEntityVars + nuis.idName,
              applicable = Applicable.allow(acc.applicable)
            )
          } else {
            Acc.deny(acc)
          }
        }

      // ======================================================================
      // Updating plans
      // ======================================================================
      case mun: MergeUniqueNode =>
        TraverseChildrenBU { acc =>
          val exprs = mun.seekExpressions
          val usedVariables = exprs.flatMap(_.dependencies).toSet
          if (usedVariables.subsetOf(inputVars)) {
            acc.copy(
              raidExpressions = acc.raidExpressions ++ exprs,
              raidEntityVars = acc.raidEntityVars + mun.idName,
              applicable = Applicable.allow(acc.applicable)
            )
          } else {
            Acc.deny(acc)
          }
        }

      case mi: MergeInto =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(mi.leftNode) && acc.raidEntityVars.contains(mi.rightNode)) {
            acc.copy(
              raidEntityVars =
                acc.raidEntityVars + mi.idName, // NOTE: In case of a match, the relationship may not be locked
              applicable = Applicable.allow(acc.applicable)
            )
          } else {
            Acc.deny(acc)
          }
        }
      case s: SetNodeProperty =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(s.idName)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case s: SetNodeProperties =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(s.idName)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case s: SetNodePropertiesFromMap =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(s.idName)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case s: SetRelationshipProperty =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(s.idName)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case s: SetRelationshipProperties =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(s.idName)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case s: SetRelationshipPropertiesFromMap =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(s.idName)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case SetDynamicProperty(_, v: LogicalVariable, _, _) =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(v)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case s: SetLabels =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(s.idName)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case r: RemoveLabels =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(r.idName)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case create: Create =>
        TraverseChildrenBU { acc =>
          if (
            create.commands.forall {
              case _: CreateNode =>
                true
              case r: CreateRelationship =>
                acc.raidEntityVars.contains(r.leftNode) && acc.raidEntityVars.contains(r.rightNode)
            }
          ) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case DeleteNode(_, v: LogicalVariable) =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(v)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case DetachDeleteNode(_, v: LogicalVariable) =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(v)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      case DeleteRelationship(_, v: LogicalVariable) =>
        TraverseChildrenBU { acc =>
          if (acc.raidEntityVars.contains(v)) {
            Acc.allow(acc)
          } else {
            Acc.deny(acc)
          }
        }
      // ======================================================================
      // Denied updating plans
      // ======================================================================
      case _: Merge | _: FusedMerge =>
        TraverseChildrenBU { acc =>
          Acc.deny(acc)
        }
      case _: Foreach | _: ForeachApply =>
        TraverseChildrenBU { acc =>
          Acc.deny(acc)
        }
      case _: LockNodes =>
        TraverseChildrenBU { acc =>
          Acc.deny(acc)
        }
      case _: SetProperty | _: SetProperties | _: SetPropertiesFromMap | _: SetDynamicProperty =>
        // Deny, since we would have to evaluate the expressions to figure this out, but it may not be possible until runtime.
        // NOTE: We still allow a specific case of SetDynamicProperty in a clause above (when the expression is a variable)
        TraverseChildrenBU { acc =>
          Acc.deny(acc)
        }
      case _: DeleteNode | _: DetachDeleteNode | _: DeleteRelationship | _: DeleteExpression | _: DetachDeleteExpression | _: DeletePath | _: DetachDeletePath =>
        // Deny, since we would have to evaluate the expressions to figure this out, but it may not be possible until runtime.
        TraverseChildrenBU { acc =>
          Acc.deny(acc)
        }
    }
    result
  }

  sealed private trait Applicable
  private case object NotApplicable extends Applicable
  private case object Allowed extends Applicable
  private case object Denied extends Applicable

  private object Applicable {

    def allow(a: Applicable): Applicable = a match {
      case NotApplicable => Allowed
      case _             => a
    }
    def deny(a: Applicable): Applicable = Denied
  }

  private case class Acc(
    raidExpressions: ListSet[Expression],
    raidEntityVars: ListSet[LogicalVariable],
    applicable: Applicable
  )

  private object Acc {
    val empty: Acc = Acc(ListSet.empty[Expression], ListSet.empty[LogicalVariable], NotApplicable)
    def allow(acc: Acc): Acc = acc.copy(applicable = Applicable.allow(acc.applicable))
    def deny(acc: Acc): Acc = acc.copy(applicable = Applicable.deny(acc.applicable))
  }
}
