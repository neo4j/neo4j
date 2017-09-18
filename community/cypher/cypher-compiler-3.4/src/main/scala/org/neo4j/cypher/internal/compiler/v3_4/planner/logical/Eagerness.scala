/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.ir.v3_4.{IdName, PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.v3_4.logical.plans.{Apply, LogicalPlan, NodeLogicalLeafPlan}

import scala.annotation.tailrec

object Eagerness {

  /**
   * Determines whether there is a conflict between the so-far planned LogicalPlan
   * and the remaining parts of the PlannerQuery. This function assumes that the
   * argument PlannerQuery is the very head of the PlannerQuery chain.
   */
  def readWriteConflictInHead(plan: LogicalPlan, plannerQuery: PlannerQuery): Boolean = {
    // The first leaf node is always reading through a stable iterator.
    // We will only consider this analysis for all other node iterators.
    val unstableLeaves = plan.leaves.collect {
      case n: NodeLogicalLeafPlan => n.idName
    }

    if (unstableLeaves.isEmpty)
      false // the query did not start with a read, possibly CREATE () ...
    else
      // Start recursion by checking the given plannerQuery against itself
      headConflicts(plannerQuery, plannerQuery, unstableLeaves.tail)
  }

  @tailrec
  private def headConflicts(head: PlannerQuery, tail: PlannerQuery, unstableLeaves: Seq[IdName]): Boolean = {
    val mergeReadWrite = head == tail && head.queryGraph.containsMergeRecursive
    val conflict = if (tail.queryGraph.readOnly || mergeReadWrite) false
    else {
      //if we have unsafe rels we need to check relation overlap and delete
      //overlap immediately
      (hasUnsafeRelationships(head.queryGraph) &&
        (tail.queryGraph.createRelationshipOverlap(head.queryGraph) ||
          tail.queryGraph.deleteOverlap(head.queryGraph) ||
          tail.queryGraph.setPropertyOverlap(head.queryGraph))
        ) ||
        //otherwise only do checks if we have more that one leaf
        unstableLeaves.exists(
          nodeOverlap(_, head.queryGraph, tail) ||
            tail.queryGraph.createRelationshipOverlap(head.queryGraph) ||
            tail.queryGraph.setLabelOverlap(head.queryGraph) || // TODO:H Verify. Pontus did this a bit differently
            tail.queryGraph.setPropertyOverlap(head.queryGraph) ||
            tail.queryGraph.deleteOverlap(head.queryGraph) ||
            tail.queryGraph.foreachOverlap(head.queryGraph))
    }
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      headConflicts(head, tail.tail.get, unstableLeaves)
  }

  def headReadWriteEagerize(inputPlan: LogicalPlan, query: PlannerQuery)
                           (implicit context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || readWriteConflictInHead(inputPlan, query))
      context.logicalPlanProducer.planEager(inputPlan)
    else
      inputPlan
  }

  def tailReadWriteEagerizeNonRecursive(inputPlan: LogicalPlan, query: PlannerQuery)
                           (implicit context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || readWriteConflict(query, query))
      context.logicalPlanProducer.planEager(inputPlan)
    else
      inputPlan
  }

  // NOTE: This does not check conflict within the query itself (like tailReadWriteEagerizeNonRecursive)
  def tailReadWriteEagerizeRecursive(inputPlan: LogicalPlan, query: PlannerQuery)
                           (implicit context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || (query.tail.isDefined && readWriteConflictInTail(query, query.tail.get)))
      context.logicalPlanProducer.planEager(inputPlan)
    else
      inputPlan
  }

  def headWriteReadEagerize(inputPlan: LogicalPlan, query: PlannerQuery)
                       (implicit context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val conflictInHorizon = query.queryGraph.overlapsHorizon(query.horizon, context.semanticTable)
    if (alwaysEager || conflictInHorizon || query.tail.isDefined && writeReadConflictInHead(query, query.tail.get))
      context.logicalPlanProducer.planEager(inputPlan)
    else
      inputPlan
  }

  def tailWriteReadEagerize(inputPlan: LogicalPlan, query: PlannerQuery)
                             (implicit context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val conflictInHorizon = query.queryGraph.overlapsHorizon(query.horizon, context.semanticTable)
    if (alwaysEager || conflictInHorizon || query.tail.isDefined && writeReadConflictInTail(query, query.tail.get))
      context.logicalPlanProducer.planEager(inputPlan)
    else
      inputPlan
  }

  def horizonReadWriteEagerize(inputPlan: LogicalPlan, query: PlannerQuery)
                              (implicit context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || (query.tail.nonEmpty && horizonReadWriteConflict(query, query.tail.get)))
      context.logicalPlanProducer.planEager(inputPlan)
    else
      inputPlan
  }

  /**
    * Determines whether there is a conflict between the two PlannerQuery objects.
    * This function assumes that none of the argument PlannerQuery objects is
    * the head of the PlannerQuery chain.
    */
  @tailrec
  def readWriteConflictInTail(head: PlannerQuery, tail: PlannerQuery): Boolean = {
    val conflict = readWriteConflict(head, tail)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      readWriteConflictInTail(head, tail.tail.get)
  }

  def readWriteConflict(readQuery: PlannerQuery, writeQuery: PlannerQuery): Boolean = {
    val mergeReadWrite = readQuery == writeQuery && readQuery.queryGraph.containsMergeRecursive
    val conflict =
      if (writeQuery.queryGraph.readOnly || mergeReadWrite)
        false
      else
        writeQuery.queryGraph overlaps readQuery.queryGraph
    conflict
  }

  @tailrec
  def writeReadConflictInTail(head: PlannerQuery, tail: PlannerQuery)
                             (implicit context: LogicalPlanningContext): Boolean = {
    val conflict =
      if (tail.queryGraph.writeOnly) false
      else (head.queryGraph overlaps tail.queryGraph) ||
        head.queryGraph.overlapsHorizon(tail.horizon, context.semanticTable) ||
        deleteReadOverlap(head.queryGraph, tail.queryGraph)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      writeReadConflictInTail(head, tail.tail.get)
  }

  @tailrec
  def horizonReadWriteConflict(head: PlannerQuery, tail: PlannerQuery)
                              (implicit context: LogicalPlanningContext): Boolean = {
    val conflict = tail.queryGraph.overlapsHorizon(head.horizon, context.semanticTable)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      horizonReadWriteConflict(head, tail.tail.get)
  }

  private def deleteReadOverlap(from: QueryGraph, to: QueryGraph)(implicit context: LogicalPlanningContext): Boolean = {
    val deleted = from.identifiersToDelete
    deletedRelationshipsOverlap(deleted, to) || deletedNodesOverlap(deleted, to)
  }

  private def deletedRelationshipsOverlap(deleted: Set[IdName], to: QueryGraph)
                                         (implicit context: LogicalPlanningContext): Boolean = {
    val relsToRead = to.allPatternRelationshipsRead
    val relsDeleted = deleted.filter(id => context.semanticTable.isRelationship(id.name))
    relsToRead.nonEmpty && relsDeleted.nonEmpty
  }

  private def deletedNodesOverlap(deleted: Set[IdName], to: QueryGraph)
                                 (implicit context: LogicalPlanningContext): Boolean = {
    val nodesToRead = to.allPatternNodesRead
    val nodesDeleted = deleted.filter(id => context.semanticTable.isNode(id.name))
    nodesToRead.nonEmpty && nodesDeleted.nonEmpty
  }

  def writeReadConflictInHead(head: PlannerQuery, tail: PlannerQuery)
                             (implicit context: LogicalPlanningContext): Boolean = {
    // If the first planner query is write only, we can use a different overlaps method (writeOnlyHeadOverlaps)
    // that makes us less eager
    if (head.queryGraph.writeOnly)
      writeReadConflictInHeadRecursive(head, tail)
    else
      writeReadConflictInTail(head, tail)
  }

  @tailrec
  def writeReadConflictInHeadRecursive(head: PlannerQuery, tail: PlannerQuery): Boolean = {
    // TODO:H Refactor: This is same as writeReadConflictInTail, but with different overlaps method. Pass as a parameter
    val conflict =
      if (tail.queryGraph.writeOnly) false
      else
        // NOTE: Here we do not check writeOnlyHeadOverlapsHorizon, because we do not know of any case where a
        // write-only head could cause problems with reads in future horizons
        head.queryGraph writeOnlyHeadOverlaps tail.queryGraph

    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      writeReadConflictInHeadRecursive(head, tail.tail.get)
  }

  /*
   * Check if the labels or properties of the node with the provided IdName overlaps
   * with the labels or properties updated in this query. This may cause the read to affected
   * by the writes.
   */
  private def nodeOverlap(currentNode: IdName, headQueryGraph: QueryGraph, tail: PlannerQuery): Boolean = {
    val labelsOnCurrentNode = headQueryGraph.allKnownLabelsOnNode(currentNode)
    val propertiesOnCurrentNode = headQueryGraph.allKnownPropertiesOnIdentifier(currentNode).map(_.propertyKey)
    val labelsToCreate = tail.queryGraph.createLabels
    val propertiesToCreate = tail.queryGraph.createNodeProperties
    val labelsToRemove = tail.queryGraph.labelsToRemoveFromOtherNodes(currentNode)

    val tailCreatesNodes = tail.exists(_.queryGraph.createsNodes)
    tail.queryGraph.updatesNodes &&
      (labelsOnCurrentNode.isEmpty && propertiesOnCurrentNode.isEmpty && tailCreatesNodes || //MATCH () CREATE/MERGE (...)?
        (labelsOnCurrentNode intersect labelsToCreate).nonEmpty || //MATCH (:A) CREATE (:A)?
        propertiesOnCurrentNode.exists(propertiesToCreate.overlaps) || //MATCH ({prop:42}) CREATE ({prop:...})

        //MATCH (n:A), (m:B) REMOVE n:B
        //MATCH (n:A), (m:A) REMOVE m:A
        (labelsToRemove intersect labelsOnCurrentNode).nonEmpty
        )
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

  case object unnestEager extends Rewriter {

    /*
    Based on unnestApply (which references a paper)

    This rewriter does _not_ adhere to the contract of moving from a valid
    plan to a valid plan, but it is crucial to get eager plans placed correctly.

    Glossary:
      Ax : Apply
      L,R: Arbitrary operator, named Left and Right
      SR : SingleRow - operator that produces single row with no columns
      CN : CreateNode
      Dn : Delete node
      Dr : Delete relationship
      E : Eager
      Sp : SetProperty
      Sm : SetPropertiesFromMap
      Sl : SetLabels
      U : Unwind
     */

    private val instance: Rewriter = fixedPoint(bottomUp(Rewriter.lift {

      // L Ax (E R) => E Ax (L R)
      case apply@Apply(lhs, eager@Eager(inner)) =>
        eager.copy(inner = Apply(lhs, inner)(apply.solved))(apply.solved)

      // L Ax (CN R) => CN Ax (L R)
      case apply@Apply(lhs, create@CreateNode(rhs, name, labels, props)) =>
        create.copy(source = Apply(lhs, rhs)(apply.solved), name, labels, props)(apply.solved)

      // L Ax (CR R) => CR Ax (L R)
      case apply@Apply(lhs, create@CreateRelationship(rhs, _, _, _, _, _)) =>
        create.copy(source = Apply(lhs, rhs)(apply.solved))(apply.solved)

      // L Ax (Dn R) => Dn Ax (L R)
      case apply@Apply(lhs, delete@DeleteNode(rhs, expr)) =>
        delete.copy(source = Apply(lhs, rhs)(apply.solved), expr)(apply.solved)

      // L Ax (Dn R) => Dn Ax (L R)
      case apply@Apply(lhs, delete@DetachDeleteNode(rhs, expr)) =>
        delete.copy(source = Apply(lhs, rhs)(apply.solved), expr)(apply.solved)

      // L Ax (Dr R) => Dr Ax (L R)
      case apply@Apply(lhs, delete@DeleteRelationship(rhs, expr)) =>
        delete.copy(source = Apply(lhs, rhs)(apply.solved), expr)(apply.solved)

      // L Ax (Sp R) => Sp Ax (L R)
      case apply@Apply(lhs, set@SetNodeProperty(rhs, idName, key, value)) =>
        set.copy(source = Apply(lhs, rhs)(apply.solved), idName, key, value)(apply.solved)

      // L Ax (Sm R) => Sm Ax (L R)
      case apply@Apply(lhs, set@SetNodePropertiesFromMap(rhs, idName, expr, removes)) =>
        set.copy(source = Apply(lhs, rhs)(apply.solved), idName, expr, removes)(apply.solved)

      // L Ax (Sl R) => Sl Ax (L R)
      case apply@Apply(lhs, set@SetLabels(rhs, idName, labelNames)) =>
        set.copy(source = Apply(lhs, rhs)(apply.solved), idName, labelNames)(apply.solved)

      // L Ax (Rl R) => Rl Ax (L R)
      case apply@Apply(lhs, remove@RemoveLabels(rhs, idName, labelNames)) =>
        remove.copy(source = Apply(lhs, rhs)(apply.solved), idName, labelNames)(apply.solved)

    }))

    override def apply(input: AnyRef): AnyRef = instance.apply(input)
  }
}
