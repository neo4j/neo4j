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
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.UnnestingRewriter
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.qgWithNoStableIdentifierAndOnlyLeaves
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.Apply
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

object Eagerness {

  /**
   * Determines whether there is a conflict between the so-far planned LogicalPlan
   * and the remaining parts of the PlannerQuery. This function assumes that the
   * argument PlannerQuery is the very head of the PlannerQuery chain.
   */
  private def readWriteConflictInHead(plan: LogicalPlan, plannerQuery: SinglePlannerQuery, context: LogicalPlanningContext): Boolean = {
    val nodeOrRelLeaves: Seq[LogicalLeafPlan] = plan.leaves.collect {
      case n: NodeLogicalLeafPlan => n
      case r: RelationshipLogicalLeafPlan => r
    }

    if (nodeOrRelLeaves.isEmpty)
      false // the query did not start with a read, possibly CREATE () ...
    else {
      // The first leaf node is always reading through a stable iterator.
      // Collect all predicates solved by that leaf and exclude them from the eagerness analysis.
      val stablySolvedPredicates: Set[Predicate] = nodeOrRelLeaves.headOption.map { p =>
          context.planningAttributes.solveds(p.id).asSinglePlannerQuery.queryGraph.selections.predicates
      }.getOrElse(Set.empty[Predicate])

      // We still need to distinguish the stable leaf from the others, and denote whether it is id-stable (== IdSeek).
      val stableIdentifier = nodeOrRelLeaves.headOption.map {
        case NodeByIdSeek(idName, _, _) => QgWithLeafInfo.StableIdentifier(idName, isIdStable = true)
        case DirectedRelationshipByIdSeek(idName, _, _, _, _) => QgWithLeafInfo.StableIdentifier(idName, isIdStable = true)
        case UndirectedRelationshipByIdSeek(idName, _, _, _, _) => QgWithLeafInfo.StableIdentifier(idName, isIdStable = true)

        case n: NodeLogicalLeafPlan => QgWithLeafInfo.StableIdentifier(n.idName, isIdStable = false)
        case r: RelationshipLogicalLeafPlan => QgWithLeafInfo.StableIdentifier(r.idName, isIdStable = false)
      }
      val unstableLeaves = nodeOrRelLeaves.tail.map {
        case n: NodeLogicalLeafPlan => n.idName
        case r: RelationshipLogicalLeafPlan => r.idName
      }.toSet

      val headQgWithLeafInfo = QgWithLeafInfo(
        plannerQuery.queryGraph,
        stablySolvedPredicates,
        unstableLeaves = unstableLeaves,
        stableIdentifier = stableIdentifier)

      // Start recursion by checking the given plannerQuery against itself
      headConflicts(plannerQuery, plannerQuery, headQgWithLeafInfo)
    }
  }

  @tailrec
  private def headConflicts(head: SinglePlannerQuery, tail: SinglePlannerQuery, headQgWithLeafInfo: QgWithLeafInfo): Boolean = {
    def overlapsHead(queryGraph: QueryGraph): Boolean = queryGraph overlaps headQgWithLeafInfo

    val conflictWithQqInHorizon = tail.horizon.allQueryGraphs.exists(qg => overlapsHead(qg))
    val mergeReadWrite = head == tail && head.queryGraph.containsMergeRecursive

    val conflict = {
      if (conflictWithQqInHorizon)
        true
      else if (tail.queryGraph.readOnly || mergeReadWrite)
        false
      else
        overlapsHead(tail.queryGraph)
    }

    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      headConflicts(head, tail.tail.get, headQgWithLeafInfo)
  }

  def headReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || readWriteConflictInHead(inputPlan, query, context))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def tailReadWriteEagerizeNonRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || readWriteConflict(query, query))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  // NOTE: This does not check conflict within the query itself (like tailReadWriteEagerizeNonRecursive)
  def tailReadWriteEagerizeRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || (query.tail.isDefined && readWriteConflictInTail(query, query.tail.get)))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def headWriteReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val conflictInHorizon = query.queryGraph.overlapsHorizon(query.horizon, context.semanticTable)
    if (alwaysEager || conflictInHorizon || query.tail.isDefined && writeReadConflictInHead(query, query.tail.get, context))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def tailWriteReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val conflictInHorizon = query.queryGraph.overlapsHorizon(query.horizon, context.semanticTable)
    if (alwaysEager || conflictInHorizon || query.tail.isDefined && writeReadConflictInTail(query, query.tail.get, context))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def horizonEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val pcWrappedPlan = inputPlan match {
      case ProcedureCall(left, call) if call.signature.eager =>
        context.logicalPlanProducer.planProcedureCall(context.logicalPlanProducer.planEager(left, context), call, context)
      case _ =>
        inputPlan
    }

    val conflict = alwaysEager ||
      horizonReadWriteConflict(query, context) ||
      horizonWriteReadConflict(query)

    if (conflict) {
      context.logicalPlanProducer.planEager(pcWrappedPlan, context)
    } else {
      pcWrappedPlan
    }
  }

  private def horizonReadWriteConflict(query: SinglePlannerQuery, context: LogicalPlanningContext): Boolean = {
    query.tail.nonEmpty && horizonReadWriteConflictRecursive(query.horizon, query.tail.get, context.semanticTable)
  }

  private def horizonWriteReadConflict(query: SinglePlannerQuery): Boolean = {
    val horizonQgs = query.horizon.allQueryGraphs
    val tailQgs = query.tail.toSeq.flatMap(_.allQueryGraphs)

    tailQgs.map(qgWithNoStableIdentifierAndOnlyLeaves).exists(readQg => horizonQgs.exists(writeQg => writeQg.overlaps(readQg)))
  }

  /**
   * Determines whether there is a conflict between the two PlannerQuery objects.
   * This function assumes that none of the argument PlannerQuery objects is
   * the head of the PlannerQuery chain.
   */
  @tailrec
  def readWriteConflictInTail(head: SinglePlannerQuery, tail: SinglePlannerQuery): Boolean = {
    val conflict = readWriteConflict(head, tail)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      readWriteConflictInTail(head, tail.tail.get)
  }

  def readWriteConflict(readQuery: SinglePlannerQuery, writeQuery: SinglePlannerQuery): Boolean = {
    val qgWithLeafInfo = qgWithNoStableIdentifierAndOnlyLeaves(readQuery.queryGraph)

    val conflictWithQgInHorizon = writeQuery.horizon.allQueryGraphs.exists(qg => qg overlaps qgWithLeafInfo)
    val mergeReadWrite = readQuery == writeQuery && readQuery.queryGraph.containsMergeRecursive

    if (conflictWithQgInHorizon)
      true
    else if (writeQuery.queryGraph.readOnly || mergeReadWrite)
      false
    else
      writeQuery.queryGraph overlaps qgWithLeafInfo
  }

  @tailrec
  def writeReadConflictInTail(head: SinglePlannerQuery, tail: SinglePlannerQuery, context: LogicalPlanningContext): Boolean = {
    val tailQgWithLeafInfo = qgWithNoStableIdentifierAndOnlyLeaves(tail.queryGraph)
    val conflict =
      if (tail.queryGraph.writeOnly) false
      else (head.queryGraph overlaps tailQgWithLeafInfo) ||
        head.queryGraph.overlapsHorizon(tail.horizon, context.semanticTable) ||
        deleteReadOverlap(head.queryGraph, tail.queryGraph, context)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      writeReadConflictInTail(head, tail.tail.get, context)
  }

  @tailrec
  def horizonReadWriteConflictRecursive(headHorizon: QueryHorizon, tail: SinglePlannerQuery, semanticTable: SemanticTable): Boolean = {
    val conflict = tail.queryGraph.overlapsHorizon(headHorizon, semanticTable)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      horizonReadWriteConflictRecursive(headHorizon, tail.tail.get, semanticTable)
  }

  private def deleteReadOverlap(from: QueryGraph, to: QueryGraph, context: LogicalPlanningContext): Boolean = {
    val deleted = from.identifiersToDelete
    deletedRelationshipsOverlap(deleted, to, context) || deletedNodesOverlap(deleted, to, context)
  }

  private def deletedRelationshipsOverlap(deleted: Set[String], to: QueryGraph, context: LogicalPlanningContext): Boolean = {
    val relsToRead = to.allPatternRelationshipsRead
    val relsDeleted = deleted.filter(id => context.semanticTable.isRelationship(id))
    relsToRead.nonEmpty && relsDeleted.nonEmpty
  }

  private def deletedNodesOverlap(deleted: Set[String], to: QueryGraph, context: LogicalPlanningContext): Boolean = {
    val nodesToRead = to.allPatternNodesRead
    val nodesDeleted = deleted.filter(id => context.semanticTable.isNode(id))
    nodesToRead.nonEmpty && nodesDeleted.nonEmpty
  }

  def writeReadConflictInHead(head: SinglePlannerQuery, tail: SinglePlannerQuery, context: LogicalPlanningContext): Boolean = {
    // If the first planner query is write only, we can use a different overlaps method (writeOnlyHeadOverlaps)
    // that makes us less eager
    if (head.queryGraph.writeOnly)
      writeReadConflictInHeadRecursive(head, tail)
    else
      writeReadConflictInTail(head, tail, context)
  }

  @tailrec
  def writeReadConflictInHeadRecursive(head: SinglePlannerQuery, tail: SinglePlannerQuery): Boolean = {
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
      case apply@Apply(lhs, eager: Eager, fromSubquery) if !fromSubquery =>
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
