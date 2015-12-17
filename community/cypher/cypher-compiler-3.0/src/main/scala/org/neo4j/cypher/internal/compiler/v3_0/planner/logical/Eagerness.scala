/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{IdName, LogicalPlan, NodeLogicalLeafPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.{PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection

import scala.annotation.tailrec

object Eagerness {

  /**
   * Determines whether there is a conflict between the so-far planned LogicalPlan
   * and the remaining parts of the PlannerQuery. This function assumes that the
   * argument PlannerQuery is the very head of the PlannerQuery chain.
   */
  def conflictInHead(plan: LogicalPlan, plannerQuery: PlannerQuery): Boolean = {
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
    val conflict = if (tail.updateGraph.isEmpty) false
    else {
      //if we have unsafe rels we need to check relation overlap and delete
      //overlap immediately
      (hasUnsafeRelationships(head.queryGraph) &&
        (tail.updateGraph.createRelationshipOverlap(head.queryGraph) ||
          tail.updateGraph.deleteOverlap(head.queryGraph) ||
          tail.updateGraph.setPropertyOverlap(head.queryGraph))
        ) ||
        //otherwise only do checks if we have more that one leaf
        unstableLeaves.exists(
          nodeOverlap(_, head.queryGraph, tail) ||
            tail.updateGraph.createRelationshipOverlap(head.queryGraph) ||
            tail.updateGraph.setLabelOverlap(head.queryGraph) ||
            tail.updateGraph.setPropertyOverlap(head.queryGraph) ||
            tail.updateGraph.deleteOverlap(head.queryGraph))
    }
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      headConflicts(head, tail.tail.get, unstableLeaves)
  }


  /**
   * Determines whether there is a conflict between the so-far planned LogicalPlan
   * and the remaining parts of the PlannerQuery. This function assumes that the
   * argument PlannerQuery is not the head of the PlannerQuery chain.
   */
  def conflictInTail(plan: LogicalPlan, plannerQuery: PlannerQuery): Boolean = {
    // Start recursion by checking the given plannerQuery against itself
    tailConflicts(plannerQuery, plannerQuery)
  }

  @tailrec
  private def tailConflicts(head: PlannerQuery, tail: PlannerQuery): Boolean = {
    val conflict = if (tail.updateGraph.isEmpty) false
    else tail.updateGraph overlaps head.queryGraph
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      tailConflicts(head, tail.tail.get)
  }

  /*
   * Check if the labels or properties of the node with the provided IdName overlaps
   * with the labels or properties updated in this query. This may cause the read to affected
   * by the writes.
   */
  private def nodeOverlap(currentNode: IdName, headQueryGraph: QueryGraph, tail: PlannerQuery): Boolean = {
    val labelsOnCurrentNode = headQueryGraph.allKnownLabelsOnNode(currentNode).toSet
    val propertiesOnCurrentNode = headQueryGraph.allKnownPropertiesOnIdentifier(currentNode).map(_.propertyKey)
    val labelsToCreate = tail.updateGraph.createLabels
    val propertiesToCreate = tail.updateGraph.createNodeProperties
    val labelsToRemove = tail.updateGraph.labelsToRemove

    tail.updatesNodes &&
      (labelsOnCurrentNode.isEmpty && propertiesOnCurrentNode.isEmpty && tail.createsNodes || //MATCH () CREATE (...)?
        (labelsOnCurrentNode intersect labelsToCreate).nonEmpty || //MATCH (:A) CREATE (:A)?
        propertiesOnCurrentNode.exists(propertiesToCreate.overlaps) || //MATCH ({prop:42}) CREATE ({prop:...})

        //MATCH (n:A), (m:B) REMOVE n:B
        //MATCH (n:A), (m:A) REMOVE m:A
        (labelsToRemove intersect labelsOnCurrentNode).nonEmpty)
  }

  /*
   * Unsafe relationships are what may cause unstable
   * iterators when expanding. The unsafe cases are:
   * - (a)-[r]-(b) (undirected)
   * - (a)-[r1]->(b)-[r2]->(c) (multi step)
   * - (a)-[r*]->(b) (variable length)
   */
  private def hasUnsafeRelationships(queryGraph: QueryGraph): Boolean = {
    val allPatterns = queryGraph.allPatternRelationships
    allPatterns.size > 1 || allPatterns.exists(r => r.dir == SemanticDirection.BOTH || !r.length.isSimple)
  }

}
