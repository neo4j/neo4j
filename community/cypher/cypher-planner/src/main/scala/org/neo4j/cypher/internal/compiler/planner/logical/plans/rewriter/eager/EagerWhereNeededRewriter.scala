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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.BestPositionFinder.pickPlansToEagerize
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.CandidateListFinder.findCandidateLists
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.EagerWhereNeededRewriter.ChildrenIds
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.EagerWhereNeededRewriter.summarizeEagernessReasonsRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.collectReadsAndWrites
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.ReasonWithConflict
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalUnaryPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.topDown

import scala.collection.immutable.BitSet
import scala.collection.mutable

/**
 * Insert Eager only where it's needed to maintain correct semantics.
 */
case class EagerWhereNeededRewriter(
  cardinalities: Cardinalities,
  attributes: Attributes[LogicalPlan],
  shouldCompressReasons: Boolean,
  cancellationChecker: CancellationChecker
) extends EagerRewriter(attributes) {

  override def eagerize(
    plan: LogicalPlan,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): LogicalPlan = {

    implicit val childrenIds: ChildrenIds = new ChildrenIds

    // Step 1: Find reads and writes
    val readsAndWrites =
      collectReadsAndWrites(plan, semanticTable, anonymousVariableNameGenerator, childrenIds, cancellationChecker)

    // Step 2: Find conflicting plans
    val conflicts = ConflictFinder.withCaching().findConflictingPlans(readsAndWrites, plan)

    // Step 3: Find candidate lists where Eager can be planned
    val candidateLists = findCandidateLists(plan, conflicts)

    // Step 4: Pick the best candidate in each sequence.
    val plansToEagerize = pickPlansToEagerize(cardinalities, candidateLists)

    // Step 5: Actually insert Eager operators
    val insertEagerRewriter = bottomUp(Rewriter.lift {
      case p: LogicalPlan if plansToEagerize.contains(p.id) =>
        eagerOnTopOf(p, plansToEagerize(p.id))
    })

    val rewritingSteps = Seq(
      insertEagerRewriter
    ) ++
      // Step 6: Optionally, compress reported eagerness reasons.
      Option.when(shouldCompressReasons)(summarizeEagernessReasonsRewriter)

    plan.endoRewrite(inSequence(rewritingSteps: _*))
  }
}

object EagerWhereNeededRewriter {

  private[eager] val summarizeEagernessReasonsRewriter: Rewriter = topDown(Rewriter.lift {
    case e: Eager if e.reasons.size > 1 =>
      val (reasonsWithoutConflicts, reasonsWithConflictsSummary) =
        e.reasons.foldLeft((e.reasons.empty, EagernessReason.Summarized.empty)) {
          case ((withoutConflicts, summary), r: ReasonWithConflict) => (withoutConflicts, summary.addReason(r))
          case ((withoutConflicts, summary), r)                     => (withoutConflicts + r, summary)
        }

      e.copy(reasons = reasonsWithoutConflicts + reasonsWithConflictsSummary)(SameId(e.id))
  })

  private[eager] trait PlanChildrenLookup {

    /**
     * @return `true` if `child` is a child of `plan`
     */
    def hasChild(plan: LogicalPlan, child: LogicalPlan): Boolean

    /**
     * Finds the plan among `plans` that is most downstream.
     */
    def mostDownstreamPlan(plans: LogicalPlan*): LogicalPlan

    /**
     * @return `true` if `plan` is a child of a TransactionApply/TransactionForeach.
     */
    def isInTransactionalApply(plan: LogicalPlan): Boolean

    /**
     * Tests whether the plan is nested on the RHS of a binary plan that might not have initialized the rhs
     * before yielding a row.
     */
    def readMightNotBeInitialized(plan: LogicalPlan): Boolean
  }

  private[eager] class ChildrenIds extends Attribute[LogicalPlan, BitSet] with PlanChildrenLookup {

    /**
     * All plan IDs of plans that are a child of a TransactionApply/TransactionForeach.
     */
    private val transactionalApplyNestedPlans = mutable.BitSet.empty

    /**
     * All plan IDs of plans that are on the RHS of a binary plan that may yield a row before initializing the RHS.
     */
    private val yieldBeforeInitRhs = mutable.BitSet.empty

    override def hasChild(plan: LogicalPlan, child: LogicalPlan): Boolean = {
      get(plan.id).contains(child.id.x)
    }

    override def mostDownstreamPlan(plans: LogicalPlan*): LogicalPlan = {
      // Note: This only works because CompressPlanIDs is a preCondition of EagerRewriter
      plans.minBy(_.id.x)
    }

    override def isInTransactionalApply(plan: LogicalPlan): Boolean = transactionalApplyNestedPlans.contains(plan.id.x)

    override def readMightNotBeInitialized(plan: LogicalPlan): Boolean = yieldBeforeInitRhs.contains(plan.id.x)

    /**
     * This method must be called with the plans in execution order.
     * It must be called for children of a plan before it can get called on the parents.
     */
    def recordChildren(plan: LogicalPlan): Unit = {
      if (!isDefinedAt(plan.id)) {
        val childrenIds = plan match {
          case _: LogicalLeafPlan =>
            BitSet.empty

          case plan: LogicalUnaryPlan =>
            val src = plan.source.id
            get(src).incl(src.x)

          case plan: LogicalBinaryPlan =>
            val lhs = plan.left.id
            val rhs = plan.right.id
            val lhsBits = get(lhs).incl(lhs.x)
            val rhsBits = get(rhs).incl(rhs.x)
            val res = lhsBits union rhsBits
            // Update transactionalApplyNestedPlans
            plan match {
              case _: TransactionApply | _: TransactionForeach => transactionalApplyNestedPlans |= res
              case _                                           =>
            }
            // Update yieldBeforeInitRhs
            plan match {
              case _: ApplyPlan | _: CartesianProduct | _: AssertSameNode | _: AssertSameRelationship | _: RepeatOptions | _: Union =>
                yieldBeforeInitRhs |= rhsBits
              case _: LeftOuterHashJoin | _: NodeHashJoin | _: RightOuterHashJoin | _: ValueHashJoin | _: OrderedUnion =>
            }
            res

          case _ =>
            plan.lhs.fold(BitSet.empty)(lhs => get(lhs.id).incl(lhs.id.x)) union
              plan.rhs.fold(BitSet.empty)(rhs => get(rhs.id).incl(rhs.id.x))
        }
        set(plan.id, childrenIds)
      }
    }
  }
}
