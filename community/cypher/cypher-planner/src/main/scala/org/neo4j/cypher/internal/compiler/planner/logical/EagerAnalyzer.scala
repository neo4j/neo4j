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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.UnnestingRewriter
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QgWithLeafInfo
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.qgWithNoStableIdentifierAndOnlyLeaves
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.RelationshipLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.fixedPoint

import scala.annotation.tailrec

object EagerAnalyzer {

  case class unnestEager(override val solveds: Solveds,
                         override val cardinalities: Cardinalities,
                         override val providedOrders: ProvidedOrders,
                         override val attributes: Attributes[LogicalPlan]) extends Rewriter with UnnestingRewriter {

    /*
    Based on unnestApply (which references a paper)

    This rewriter does _not_ adhere to the contract of moving from a valid
    plan to a valid plan, but it is crucial to get eager plans placed correctly.

    Glossary:
      Ax : Apply
      L,R: Arbitrary operator, named Left and Right
      E : Eager
      Up : UpdatingPlan
     */

    private val instance: Rewriter = fixedPoint(bottomUp(Rewriter.lift {

      // L Ax (E R) => E Ax (L R), don't unnest when coming from a subquery
      case apply@Apply(lhs, eager: Eager, false) =>
        unnestRightUnary(apply, lhs, eager)

      //MERGE is an updating plan that cannot be moved on top of apply since
      //it is closely tied to its source plan
      case apply@Apply(_, _: Merge, _) => apply

      // L Ax (Up R) => Up Ax (L R), don't unnest when coming from a subquery
      case apply@Apply(lhs, updatingPlan: UpdatingPlan, fromSubquery) if !fromSubquery =>
        unnestRightUnary(apply, lhs, updatingPlan)
    }))

    override def apply(input: AnyRef): AnyRef = instance.apply(input)
  }
}

class EagerAnalyzer(context: LogicalPlanningContext) {

  private implicit val semanticTable: SemanticTable = context.semanticTable

  /**
   * Determines whether there is a conflict between the so-far planned LogicalPlan
   * and the remaining parts of the PlannerQuery. This function assumes that the
   * argument PlannerQuery is the very head of the PlannerQuery chain.
   */
  private def readWriteConflictInHead(plan: LogicalPlan, plannerQuery: SinglePlannerQuery): Boolean = {
    val entityProvidingLeaves: Seq[LogicalLeafPlan] = plan.leaves.collect {
      case n: NodeLogicalLeafPlan => n
      case r: RelationshipLogicalLeafPlan => r
        // If in a subquery, we consider the argument to provide us with stable identifiers
      case a: Argument if context.isInSubquery && a.argumentIds.nonEmpty => a
    }

    if (entityProvidingLeaves.isEmpty)
      false // the query did not start with a read, possibly CREATE () ...
    else {
      // In the following we determine if there are any stably solved predicates that we can leave out of the eagerness analysis.
      // The reasoning is as follows:
      // Cursors used for leaf operators iterate over a stable snapshot of the transaction state from the moment the cursor is initialized.
      // That means the very first leaf of the whole LogicalPlan can be considered stable; all other leaf cursors might get initialized multiple times.
      // The reads from predicates that are solved by that first leaf do not need to be protected against seeing conflicting writes from later in the query.

      // If we're in a subquery, the first leaf of that subquery is not actually the first leaf of the whole LogicalPlan.
      val (maybeStableLeaf, unstableLeaves) = if (context.isInSubquery) (None, entityProvidingLeaves) else (entityProvidingLeaves.headOption, entityProvidingLeaves.tail)

      // Collect all predicates solved by the first leaf and exclude them from the eagerness analysis.
      val stablySolvedPredicates: Set[Predicate] = maybeStableLeaf.map { p =>
          context.planningAttributes.solveds(p.id).asSinglePlannerQuery.queryGraph.selections.predicates
      }.getOrElse(Set.empty[Predicate])

      // We still need to distinguish the stable leaf from the others, and denote whether it is id-stable (== IdSeek).
      val stableIdentifier = maybeStableLeaf.map {
        case NodeByIdSeek(idName, _, _) => QgWithLeafInfo.StableIdentifier(idName, isIdStable = true)
        case DirectedRelationshipByIdSeek(idName, _, _, _, _) => QgWithLeafInfo.StableIdentifier(idName, isIdStable = true)
        case UndirectedRelationshipByIdSeek(idName, _, _, _, _) => QgWithLeafInfo.StableIdentifier(idName, isIdStable = true)

        case n: NodeLogicalLeafPlan => QgWithLeafInfo.StableIdentifier(n.idName, isIdStable = false)
        case r: RelationshipLogicalLeafPlan => QgWithLeafInfo.StableIdentifier(r.idName, isIdStable = false)
      }

      val unstableLeafIdNames = unstableLeaves.flatMap {
        case n: NodeLogicalLeafPlan => Set(n.idName)
        case r: RelationshipLogicalLeafPlan => Set(r.idName)
        case a: Argument => a.argumentIds
      }.toSet

      val headQgWithLeafInfo = QgWithLeafInfo(
        plannerQuery.queryGraph,
        stablySolvedPredicates,
        unstableLeaves = unstableLeafIdNames,
        stableIdentifier = stableIdentifier)

      // Start recursion by checking the given plannerQuery against itself
      headConflicts(plannerQuery, plannerQuery, headQgWithLeafInfo)
    }
  }

  @tailrec
  private def headConflicts(head: SinglePlannerQuery, tail: SinglePlannerQuery, headQgWithLeafInfo: QgWithLeafInfo): Boolean = {
    val allHeadQgs = Set(headQgWithLeafInfo) ++ headQgWithLeafInfo.queryGraph.allQGsWithLeafInfo.toSet.filterNot(_.queryGraph == headQgWithLeafInfo.queryGraph)
    def overlapsHead(writeQg: QueryGraph): Boolean = allHeadQgs.exists(readQg => writeQg.overlaps(readQg))

    val conflictWithQqInHorizon = tail.horizon.allQueryGraphs.exists(qg => overlapsHead(qg.queryGraph))
    val mergeReadWrite = head == tail && head.queryGraph.containsMergeRecursive

    val conflict = {
      if (conflictWithQqInHorizon)
        true
      else if (tail.queryGraph.readOnly || mergeReadWrite)
        false
      else
        tail.queryGraph.allQGsWithLeafInfo.exists(qg => overlapsHead(qg.queryGraph))
    }

    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      headConflicts(head, tail.tail.get, headQgWithLeafInfo)
  }

  def headReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || readWriteConflictInHead(inputPlan, query))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def tailReadWriteEagerizeNonRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || readWriteConflict(query, query))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  // NOTE: This does not check conflict within the query itself (like tailReadWriteEagerizeNonRecursive)
  def tailReadWriteEagerizeRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || (query.tail.isDefined && readWriteConflictInTail(query, query.tail.get)))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def headWriteReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val conflictInHorizon = query.queryGraph.overlapsHorizon(query.horizon)
    if (alwaysEager || conflictInHorizon || query.tail.isDefined && writeReadConflictInHead(query, query.tail.get))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def tailWriteReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val conflictInHorizon = query.queryGraph.overlapsHorizon(query.horizon)
    if (alwaysEager || conflictInHorizon || query.tail.isDefined && writeReadConflictInTail(query, query.tail.get))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def horizonEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val pcWrappedPlan = inputPlan match {
      case ProcedureCall(left, call) if call.signature.eager =>
        context.logicalPlanProducer.planProcedureCall(context.logicalPlanProducer.planEager(left, context), call, context)
      case _ =>
        inputPlan
    }

    val conflict = alwaysEager ||
      horizonReadWriteConflict(query) ||
      horizonWriteReadConflict(query)

    if (conflict) {
      context.logicalPlanProducer.planEager(pcWrappedPlan, context)
    } else {
      pcWrappedPlan
    }
  }

  private def horizonReadWriteConflict(query: SinglePlannerQuery): Boolean = {
    query.tail.toSeq.flatMap(_.allQGsWithLeafInfo).exists(_.queryGraph.overlapsHorizon(query.horizon))
  }

  private def horizonWriteReadConflict(query: SinglePlannerQuery): Boolean = {
    val horizonQgs = query.horizon.allQueryGraphs.map(_.queryGraph)
    val tailQgs = query.tail.toSeq.flatMap(_.allQGsWithLeafInfo)

    tailQgs.exists(readQg => horizonQgs.exists(writeQg => writeQg.overlaps(readQg)))
  }

  /**
   * Determines whether there is a conflict between the two PlannerQuery objects.
   * This function assumes that none of the argument PlannerQuery objects is
   * the head of the PlannerQuery chain.
   */
  @tailrec
  private def readWriteConflictInTail(head: SinglePlannerQuery, tail: SinglePlannerQuery): Boolean = {
    val conflict = readWriteConflict(head, tail)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      readWriteConflictInTail(head, tail.tail.get)
  }

  private def readWriteConflict(readQuery: SinglePlannerQuery, writeQuery: SinglePlannerQuery): Boolean = {
    val readQGsWithLeafInfo = readQuery.queryGraph.allQGsWithLeafInfo
    def overlapsWithReadQg(writeQg: QueryGraph): Boolean = readQGsWithLeafInfo.exists(readQgWithLeafInfo => writeQg.overlaps(readQgWithLeafInfo))

    val conflictWithQgInHorizon = writeQuery.horizon.allQueryGraphs.map(_.queryGraph).exists(overlapsWithReadQg)
    val mergeReadWrite = readQuery == writeQuery && readQuery.queryGraph.containsMergeRecursive

    if (conflictWithQgInHorizon)
      true
    else if (writeQuery.queryGraph.readOnly || mergeReadWrite)
      false
    else
      writeQuery.queryGraph.allQGsWithLeafInfo.map(_.queryGraph).exists(overlapsWithReadQg)
  }

  @tailrec
  private def writeReadConflictInTail(head: SinglePlannerQuery, tail: SinglePlannerQuery): Boolean = {
    val readQGsWithLeafInfo = tail.queryGraph.allQGsWithLeafInfo
    def overlapsWithReadQg(writeQg: QueryGraph): Boolean = readQGsWithLeafInfo.exists(readQgWithLeafInfo => writeQg.overlaps(readQgWithLeafInfo))

    val conflict =
      if (tail.queryGraph.writeOnly) false
      else {
        val qgS = head.queryGraph.allQGsWithLeafInfo.map(_.queryGraph)
        qgS.exists(overlapsWithReadQg) ||
          qgS.exists(_.overlapsHorizon(tail.horizon)) ||
          qgS.exists(deleteReadOverlap(_, tail.queryGraph))
      }
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      writeReadConflictInTail(head, tail.tail.get)
  }

  private def deleteReadOverlap(from: QueryGraph, to: QueryGraph): Boolean = {
    val deleted = from.identifiersToDelete
    deletedRelationshipsOverlap(deleted, to) || deletedNodesOverlap(deleted, to)
  }

  private def deletedRelationshipsOverlap(deleted: Set[String], to: QueryGraph): Boolean = {
    val relsToRead = to.allPatternRelationshipsRead
    val relsDeleted = deleted.filter(id => semanticTable.isRelationship(id))
    relsToRead.nonEmpty && relsDeleted.nonEmpty
  }

  private def deletedNodesOverlap(deleted: Set[String], to: QueryGraph): Boolean = {
    val nodesToRead = to.allPatternNodesRead
    val nodesDeleted = deleted.filter(id => semanticTable.isNode(id))
    nodesToRead.nonEmpty && nodesDeleted.nonEmpty
  }

  private def writeReadConflictInHead(head: SinglePlannerQuery, tail: SinglePlannerQuery): Boolean = {
    // If the first planner query is write only, we can use a different overlaps method (writeOnlyHeadOverlaps)
    // that makes us less eager
    if (head.queryGraph.writeOnly)
      writeReadConflictInHeadRecursive(head, tail)
    else
      writeReadConflictInTail(head, tail)
  }

  @tailrec
  private def writeReadConflictInHeadRecursive(head: SinglePlannerQuery, tail: SinglePlannerQuery): Boolean = {
    // TODO:H Refactor: This is same as writeReadConflictInTail, but with different overlaps method. Pass as a parameter
    val conflict =
      if (tail.queryGraph.writeOnly) false
      else
      // NOTE: Here we do not check writeOnlyHeadOverlapsHorizon, because we do not know of any case where a
      // write-only head could cause problems with reads in future horizons
        head.queryGraph writeOnlyHeadOverlaps qgWithNoStableIdentifierAndOnlyLeaves(tail.queryGraph)

    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      writeReadConflictInHeadRecursive(head, tail.tail.get)
  }
}
