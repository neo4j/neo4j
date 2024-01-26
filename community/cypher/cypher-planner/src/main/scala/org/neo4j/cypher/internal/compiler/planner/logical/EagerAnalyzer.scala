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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QgWithLeafInfo
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UpdateGraph.LeafPlansPredicatesResolver
import org.neo4j.cypher.internal.ir.UpdateGraph.SolvedPredicatesOfOneLeafPlan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.RelationshipLogicalLeafPlan
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.exceptions.InternalException

import scala.annotation.tailrec
import scala.collection.immutable.ListSet

object EagerAnalyzer {

  def apply(context: LogicalPlanningContext): EagerAnalyzer = {
    if (context.staticComponents.readOnly) {
      return NoopEagerAnalyzer
    }

    if (context.settings.eagerAnalyzer != CypherEagerAnalyzerOption.ir) {
      return NoopEagerAnalyzer
    }

    if (context.settings.updateStrategy.alwaysEager) {
      new AlwaysEagerEagerAnalyzer(context)
    } else {
      EagerAnalyzerImpl(context)
    }
  }
}

trait EagerAnalyzer {
  def headReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan
  def tailReadWriteEagerizeNonRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan
  def tailReadWriteEagerizeRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan
  def writeReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan
  def horizonEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan
}

/**
 * Always plans Eager.
 */
class AlwaysEagerEagerAnalyzer(context: LogicalPlanningContext) extends EagerAnalyzer {

  private def planEager(inputPlan: LogicalPlan): LogicalPlan = context.staticComponents.logicalPlanProducer.planEager(
    inputPlan,
    context,
    ListSet(EagernessReason.UpdateStrategyEager)
  )

  override def headReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
    planEager(inputPlan)

  override def tailReadWriteEagerizeNonRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
    planEager(inputPlan)

  override def tailReadWriteEagerizeRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
    planEager(inputPlan)

  override def writeReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
    planEager(inputPlan)

  override def horizonEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
    planEager(inputPlan)
}

object EagerAnalyzerImpl {

  def apply(context: LogicalPlanningContext): EagerAnalyzer =
    new ForeachFlatteningEagerAnalyzerWrapper(new EagerAnalyzerImpl(context))

  class ForeachFlatteningEagerAnalyzerWrapper(inner: EagerAnalyzer) extends EagerAnalyzer {

    override def headReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
      inner.headReadWriteEagerize(inputPlan, query.flattenForeach)

    override def tailReadWriteEagerizeNonRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
      inner.tailReadWriteEagerizeNonRecursive(inputPlan, query.flattenForeach)

    override def tailReadWriteEagerizeRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
      inner.tailReadWriteEagerizeRecursive(inputPlan, query.flattenForeach)

    override def writeReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
      inner.writeReadEagerize(inputPlan, query.flattenForeach)

    override def horizonEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
      inner.horizonEagerize(inputPlan, query.flattenForeach)
  }
}

class EagerAnalyzerImpl private (context: LogicalPlanningContext) extends EagerAnalyzer {

  implicit private val semanticTable: SemanticTable = context.staticComponents.semanticTable

  /**
   * Get a LeafPlansPredicatesResolver that looks at the leaves of the given plan.
   */
  private def getLeafPlansPredicatesResolver(plan: LogicalPlan): LeafPlansPredicatesResolver =
    new LeafPlansPredicatesResolver {

      private lazy val allLeaves: Seq[LogicalPlan] = plan.leaves

      override def apply(entity: LogicalVariable): LeafPlansPredicatesResolver.LeafPlansWithSolvedPredicates = {
        val leaves: Seq[LogicalLeafPlan] = allLeaves.collect {
          case n: NodeLogicalLeafPlan if n.idName == entity         => n
          case r: RelationshipLogicalLeafPlan if r.idName == entity => r
        }

        leaves.map { p =>
          val solvedPredicates =
            context.staticComponents.planningAttributes.solveds(
              p.id
            ).asSinglePlannerQuery.queryGraph.selections.predicates
          SolvedPredicatesOfOneLeafPlan(solvedPredicates.map(_.expr).toSeq)
        }.toList match {
          case head :: tail => LeafPlansPredicatesResolver.LeafPlansFound(NonEmptyList(head, tail: _*))
          case Nil          => LeafPlansPredicatesResolver.NoLeafPlansFound
        }
      }
    }

  /**
   * Determines whether there is a conflict between the so-far planned LogicalPlan
   * and the remaining parts of the PlannerQuery. This function assumes that the
   * argument PlannerQuery is the very head of the PlannerQuery chain.
   */
  private def readWriteConflictInHead(
    plan: LogicalPlan,
    plannerQuery: SinglePlannerQuery
  ): ListSet[EagernessReason] = {
    val entityProvidingLeaves: Seq[LogicalLeafPlan] = plan.leaves.collect {
      case n: NodeLogicalLeafPlan         => n
      case r: RelationshipLogicalLeafPlan => r
      // If in a subquery, we consider the argument to provide us with stable identifiers
      case a: Argument if context.plannerState.isInSubquery && a.argumentIds.nonEmpty => a
    }

    if (entityProvidingLeaves.isEmpty)
      ListSet.empty // the query did not start with a read, possibly CREATE () ...
    else {
      // In the following we determine if there are any stably solved predicates that we can leave out of the eagerness analysis.
      // The reasoning is as follows:
      // Cursors used for leaf operators iterate over a stable snapshot of the transaction state from the moment the cursor is initialized.
      // That means the very first leaf of the whole LogicalPlan can be considered stable; all other leaf cursors might get initialized multiple times.
      // The reads from predicates that are solved by that first leaf do not need to be protected against seeing conflicting writes from later in the query.

      // If we're in a subquery, the first leaf of that subquery is not actually the first leaf of the whole LogicalPlan.
      val (maybeStableLeaf, unstableLeaves) =
        if (context.plannerState.isInSubquery) (None, entityProvidingLeaves)
        else (entityProvidingLeaves.headOption, entityProvidingLeaves.tail)

      // Collect all predicates solved by the first leaf and exclude them from the eagerness analysis.
      val stablySolvedPredicates: Set[Predicate] = maybeStableLeaf.map { p =>
        context.staticComponents.planningAttributes.solveds(p.id).asSinglePlannerQuery.queryGraph.selections.predicates
      }.getOrElse(Set.empty[Predicate])

      // We still need to distinguish the stable leaf from the others
      val stableIdentifier = maybeStableLeaf.map {
        case n: NodeLogicalLeafPlan         => QgWithLeafInfo.StableIdentifier(n.idName)
        case r: RelationshipLogicalLeafPlan => QgWithLeafInfo.StableIdentifier(r.idName)
        case x => throw new InternalException(
            s"Expected NodeLogicalLeafPlan or RelationshipLogicalLeafPlan but was ${x.getClass}"
          )
      }

      val unstableLeafIdNames = unstableLeaves.view.flatMap {
        case n: NodeLogicalLeafPlan         => Set(n.idName)
        case r: RelationshipLogicalLeafPlan => Set(r.idName)
        case a: Argument                    => a.argumentIds
        case x => throw new InternalException(
            s"Expected NodeLogicalLeafPlan, RelationshipLogicalLeafPlan or Argument but was ${x.getClass}"
          )
      }.to(ListSet)

      val leafPlansPredicatesResolver = getLeafPlansPredicatesResolver(plan)

      val headQgWithLeafInfo = QgWithLeafInfo(
        plannerQuery.queryGraph,
        stablySolvedPredicates,
        unstableLeaves = unstableLeafIdNames,
        stableIdentifier = stableIdentifier,
        isTerminatingProjection = plannerQuery.horizon.isTerminatingProjection
      )

      // Start recursion by checking the given plannerQuery against itself
      headConflicts(plannerQuery, plannerQuery, headQgWithLeafInfo, leafPlansPredicatesResolver)
    }
  }

  @tailrec
  private def headConflicts(
    head: SinglePlannerQuery,
    tail: SinglePlannerQuery,
    headQgWithLeafInfo: QgWithLeafInfo,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  ): ListSet[EagernessReason] = {
    // _.allQGsWithLeafInfo will remove the stable identifiers. As we want to keep the identifiers, we therefore need to treat headQgWithLeafInfo separately
    val allHeadQgsIncludingStableIterators =
      headQgWithLeafInfo +:
        headQgWithLeafInfo.queryGraph.allQGsWithLeafInfo
          .filterNot(_.queryGraph == headQgWithLeafInfo.queryGraph)
    val allHeadQgsIgnoringStableIterators = head.queryGraph.allQGsWithLeafInfo

    def overlapsHead(readQgs: Seq[QgWithLeafInfo])(writeQg: QueryGraph): ListSet[EagernessReason] =
      readQgs.view.flatMap(readQg => writeQg.overlaps(readQg, leafPlansPredicatesResolver)).to(ListSet)

    val conflictsWithQqInHorizon: ListSet[EagernessReason] = tail.horizon match {
      // if we are running CALL { ... } IN TRANSACTIONS, we cannot rely on stable iterators
      case CallSubqueryHorizon(_, _, _, Some(_)) =>
        tail.horizon.allQueryGraphs.map(_.queryGraph).view.flatMap(overlapsHead(allHeadQgsIgnoringStableIterators)).to(
          ListSet
        )
      case _ =>
        tail.horizon.allQueryGraphs.map(_.queryGraph).view.flatMap(overlapsHead(allHeadQgsIncludingStableIterators)).to(
          ListSet
        )
    }
    val mergeReadWrite = head == tail && head.queryGraph.containsMergeRecursive

    val conflicts: ListSet[EagernessReason] = {
      if (conflictsWithQqInHorizon.nonEmpty)
        conflictsWithQqInHorizon
      else if (tail.queryGraph.readOnly || mergeReadWrite)
        ListSet.empty
      else
        tail.queryGraph.allQGsWithLeafInfo.view.flatMap(qg =>
          overlapsHead(allHeadQgsIncludingStableIterators)(qg.queryGraph)
        ).to(ListSet)
    }

    if (conflicts.nonEmpty)
      conflicts
    else if (tail.tail.isEmpty)
      ListSet.empty
    else
      headConflicts(head, tail.tail.get, headQgWithLeafInfo, leafPlansPredicatesResolver)
  }

  override def headReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val conflicts = readWriteConflictInHead(inputPlan, query)
    if (conflicts.nonEmpty)
      context.staticComponents.logicalPlanProducer.planEager(inputPlan, context, conflicts)
    else
      inputPlan
  }

  override def tailReadWriteEagerizeNonRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val conflicts = readWriteConflict(query, query, getLeafPlansPredicatesResolver(inputPlan))
    if (conflicts.nonEmpty)
      context.staticComponents.logicalPlanProducer.planEager(inputPlan, context, conflicts)
    else
      inputPlan
  }

  // NOTE: This does not check conflict within the query itself (like tailReadWriteEagerizeNonRecursive)
  override def tailReadWriteEagerizeRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val conflicts =
      if (query.tail.isDefined)
        readWriteConflictInTail(query, query.tail.get, getLeafPlansPredicatesResolver(inputPlan))
      else ListSet.empty[EagernessReason]
    if (conflicts.nonEmpty)
      context.staticComponents.logicalPlanProducer.planEager(inputPlan, context, conflicts)
    else
      inputPlan
  }

  override def writeReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val conflictsWithHorizon =
      query.queryGraph.overlapsHorizon(query.horizon, getLeafPlansPredicatesResolver(inputPlan))
    val conflictsWithTail =
      if (query.tail.isDefined)
        writeReadConflict(query, query.tail.get, getLeafPlansPredicatesResolver(inputPlan))
      else Seq.empty
    val conflicts = conflictsWithHorizon ++ conflictsWithTail

    if (conflicts.nonEmpty)
      context.staticComponents.logicalPlanProducer.planEager(inputPlan, context, conflicts)
    else
      inputPlan
  }

  override def horizonEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val pcWrappedPlan = inputPlan match {
      case ProcedureCall(left, call) if call.signature.eager =>
        context.staticComponents.logicalPlanProducer.planEager(
          context.staticComponents.logicalPlanProducer.planProcedureCall(
            context.staticComponents.logicalPlanProducer.planEager(
              left,
              context,
              ListSet(EagernessReason.Unknown)
            ),
            call,
            context
          ),
          context,
          ListSet(EagernessReason.Unknown)
        )
      case _ =>
        inputPlan
    }

    val leafPlansPredicatesResolver = getLeafPlansPredicatesResolver(inputPlan)
    val conflicts =
      horizonReadWriteConflict(query, leafPlansPredicatesResolver) ++
        horizonWriteReadConflict(query, leafPlansPredicatesResolver) ++
        writeAfterCallInTransactionsConflict(query)
    if (conflicts.nonEmpty)
      context.staticComponents.logicalPlanProducer.planEager(pcWrappedPlan, context, conflicts)
    else
      pcWrappedPlan
  }

  private def horizonReadWriteConflict(
    query: SinglePlannerQuery,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  ): ListSet[EagernessReason] = {
    query.tail.toSeq.view.flatMap(_.allQGsWithLeafInfo).flatMap(_.queryGraph.overlapsHorizon(
      query.horizon,
      leafPlansPredicatesResolver
    )).to(
      ListSet
    )
  }

  private def horizonWriteReadConflict(
    query: SinglePlannerQuery,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  ): ListSet[EagernessReason] = {
    val horizonQgs = query.horizon.allQueryGraphs.map(_.queryGraph)
    val tailQgs = query.tail.toSeq.flatMap(_.allQGsWithLeafInfo).view

    tailQgs.flatMap(readQg => horizonQgs.flatMap(writeQg => writeQg.overlaps(readQg, leafPlansPredicatesResolver))).to(
      ListSet
    )
  }

  /**
   * Determines whether there is a conflict between the two PlannerQuery objects.
   * This function assumes that none of the argument PlannerQuery objects is
   * the head of the PlannerQuery chain.
   */
  @tailrec
  private def readWriteConflictInTail(
    head: SinglePlannerQuery,
    tail: SinglePlannerQuery,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  ): ListSet[EagernessReason] = {
    val conflicts = readWriteConflict(head, tail, leafPlansPredicatesResolver)
    if (conflicts.nonEmpty)
      conflicts
    else if (tail.tail.isEmpty)
      ListSet.empty
    else
      readWriteConflictInTail(head, tail.tail.get, leafPlansPredicatesResolver)
  }

  private def readWriteConflict(
    readQuery: SinglePlannerQuery,
    writeQuery: SinglePlannerQuery,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  ): ListSet[EagernessReason] = {
    val readQGsWithLeafInfo = readQuery.queryGraph.allQGsWithLeafInfo
    def overlapsWithReadQg(writeQg: QueryGraph): ListSet[EagernessReason] =
      readQGsWithLeafInfo.view.flatMap(readQgWithLeafInfo =>
        writeQg.overlaps(readQgWithLeafInfo, leafPlansPredicatesResolver)
      ).to(ListSet)

    val conflictsWithQgInHorizon =
      writeQuery.horizon.allQueryGraphs.view.map(_.queryGraph).flatMap(overlapsWithReadQg).to(ListSet)
    val mergeReadWrite = readQuery == writeQuery && readQuery.queryGraph.containsMergeRecursive

    if (conflictsWithQgInHorizon.nonEmpty)
      conflictsWithQgInHorizon
    else if (writeQuery.queryGraph.readOnly || mergeReadWrite)
      ListSet.empty
    else
      writeQuery.queryGraph.allQGsWithLeafInfo.view.map(_.queryGraph).flatMap(overlapsWithReadQg).to(ListSet)
  }

  @tailrec
  private def writeReadConflict(
    head: SinglePlannerQuery,
    tail: SinglePlannerQuery,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  ): ListSet[EagernessReason] = {
    val readQGsWithLeafInfo = tail.queryGraph.allQGsWithLeafInfo
    def overlapsWithReadQg(writeQg: QueryGraph): ListSet[EagernessReason] =
      readQGsWithLeafInfo.view.flatMap(readQgWithLeafInfo =>
        writeQg.overlaps(readQgWithLeafInfo, leafPlansPredicatesResolver)
      ).to(ListSet)

    val conflicts = {
      val qgS = head.queryGraph.allQGsWithLeafInfo.map(_.queryGraph)
      qgS.flatMap(overlapsWithReadQg) ++
        qgS.flatMap(_.overlapsHorizon(tail.horizon, leafPlansPredicatesResolver))
    }.to(ListSet)
    if (conflicts.nonEmpty)
      conflicts
    else if (tail.tail.isEmpty)
      ListSet.empty
    else
      writeReadConflict(head, tail.tail.get, leafPlansPredicatesResolver)
  }

  private def writeAfterCallInTransactionsConflict(query: SinglePlannerQuery): Option[EagernessReason] = {
    def isCallInTxHorizon(horizon: QueryHorizon): Boolean = {
      horizon match {
        case call: CallSubqueryHorizon => call.inTransactionsParameters.isDefined
        case _                         => false
      }
    }
    val hasConflict = isCallInTxHorizon(query.horizon) && {
      query.allPlannerQueries.drop(1).exists { tailQuery =>
        tailQuery.queryGraph.containsUpdates ||
        (!tailQuery.horizon.readOnly && !isCallInTxHorizon(tailQuery.horizon))
      }
    }
    if (hasConflict) Some(EagernessReason.WriteAfterCallInTransactions)
    else None
  }
}

object NoopEagerAnalyzer extends EagerAnalyzer {
  override def headReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = inputPlan

  override def tailReadWriteEagerizeNonRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
    inputPlan

  override def tailReadWriteEagerizeRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan =
    inputPlan
  override def writeReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = inputPlan

  override def horizonEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = inputPlan
}
