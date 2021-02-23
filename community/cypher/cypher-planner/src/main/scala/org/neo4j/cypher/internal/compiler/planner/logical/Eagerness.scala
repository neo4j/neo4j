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

import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QgWithLeafInfo
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v4_0.util.attribution.{Attributes, SameId}
import org.neo4j.cypher.internal.v4_0.util.helpers.fixedPoint
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, bottomUp}

import scala.annotation.tailrec

object Eagerness {

  /**
   * Determines whether there is a conflict between the so-far planned LogicalPlan
   * and the remaining parts of the PlannerQuery. This function assumes that the
   * argument PlannerQuery is the very head of the PlannerQuery chain.
   */
  def readWriteConflictInHead(plan: LogicalPlan, plannerQuery: SinglePlannerQuery, context: LogicalPlanningContext): Boolean = {
    val nodeOrRelLeaves: Seq[LogicalLeafPlan] = plan.leaves.collect {
      case n: NodeLogicalLeafPlan => n
      case d: DirectedRelationshipByIdSeek => d
      case u: UndirectedRelationshipByIdSeek => u
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
      }
      val unstableLeaves = nodeOrRelLeaves.tail.map {
      case n: NodeLogicalLeafPlan => n.idName
      case d: DirectedRelationshipByIdSeek => d.idName
      case u: UndirectedRelationshipByIdSeek => u.idName
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
    val mergeReadWrite = head == tail && head.queryGraph.containsMergeRecursive
    val conflict = if (tail.queryGraph.readOnly || mergeReadWrite) false
    else {
        tail.queryGraph.nodeOverlap(headQgWithLeafInfo) ||
        tail.queryGraph.removeLabelOverlap(headQgWithLeafInfo) ||
        tail.queryGraph.setLabelOverlap(headQgWithLeafInfo) ||
        tail.queryGraph.createRelationshipOverlap(headQgWithLeafInfo) ||
        tail.queryGraph.setPropertyOverlap(headQgWithLeafInfo) ||
        tail.queryGraph.deleteOverlap(headQgWithLeafInfo) ||
        tail.queryGraph.foreachOverlap(headQgWithLeafInfo)
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

  def horizonReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    inputPlan match {
      case ProcedureCall(left, call) if call.signature.eager =>
        context.logicalPlanProducer.planCallProcedure(context.logicalPlanProducer.planEager(left, context), call, query.interestingOrder, context)
      case _ if alwaysEager || (query.tail.nonEmpty && horizonReadWriteConflict(query, query.tail.get, context)) =>
        context.logicalPlanProducer.planEager(inputPlan, context)
      case _ =>
        inputPlan
    }
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

  /**
   * @return a QgWithLeafInfo, where there is no stable identifier. Moreover all variables are assumed to be leaves.
   */
  private def qgWithNoStableIdentifierAndOnlyLeaves(qg: QueryGraph): QgWithLeafInfo =
    QgWithLeafInfo(qg, Set.empty, qg.allCoveredIds, None)

  def readWriteConflict(readQuery: SinglePlannerQuery, writeQuery: SinglePlannerQuery): Boolean = {
    val mergeReadWrite = readQuery == writeQuery && readQuery.queryGraph.containsMergeRecursive
    val conflict =
      if (writeQuery.queryGraph.readOnly || mergeReadWrite)
        false
      else
        writeQuery.queryGraph overlaps qgWithNoStableIdentifierAndOnlyLeaves(readQuery.queryGraph)
    conflict
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
  def horizonReadWriteConflict(head: SinglePlannerQuery, tail: SinglePlannerQuery, context: LogicalPlanningContext): Boolean = {
    val conflict = tail.queryGraph.overlapsHorizon(head.horizon, context.semanticTable)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      horizonReadWriteConflict(head, tail.tail.get, context)
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


  /*
   * Unsafe relationships are what may cause unstable
   * iterators when expanding. The unsafe cases are:
   * - (a)-[r]-(b) (undirected)
   * - (a)-[r1]->(b)-[r2]->(c) (multi step)
   * - (a)-[r*]->(b) (variable length)
   * - where the match pattern and the create/merge pattern
   *   includes the same nodes, but the direction is reversed
   */
  private def hasUnsafeRelationships(queryGraph: QueryGraph): Boolean = {
    /*
     * It is difficult to implement a perfect fit for all four above rules (the fourth one is the tricky one).
     * Therefore we are better safe than sorry, and just see all relationships as unsafe.
     */
    hasRelationships(queryGraph)
  }

  private def hasRelationships(queryGraph: QueryGraph) = queryGraph.allPatternRelationships.nonEmpty

  case class unnestEager(solveds: Solveds, attributes: Attributes[LogicalPlan]) extends Rewriter {

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

      // L Ax (E R) => E Ax (L R)
      case apply@Apply(lhs, eager@Eager(inner)) =>
        val res = eager.copy(source = Apply(lhs, inner)(SameId(apply.id)))(attributes.copy(eager.id))
        solveds.copy(apply.id, res.id)
        res

     // L Ax (Up R) => Up Ax (L R)
      case apply@Apply(lhs, updatingPlan: UpdatingPlan) =>
        val res = updatingPlan.withSource(Apply(lhs, updatingPlan.source)(SameId(apply.id)))(attributes.copy(updatingPlan.id))
        solveds.copy(apply.id, res.id)
        res
    }))

    override def apply(input: AnyRef): AnyRef = instance.apply(input)
  }
}
