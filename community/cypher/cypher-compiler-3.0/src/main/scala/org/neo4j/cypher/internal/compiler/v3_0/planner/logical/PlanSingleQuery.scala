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

import org.neo4j.cypher.internal.compiler.v3_0.planner.PlannerQuery
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{IdName, LogicalPlan, NodeLogicalLeafPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.{countStorePlanner, verifyBestPlan}
import org.neo4j.cypher.internal.frontend.v3_0.{Rewriter, SemanticDirection}

/*
This coordinates PlannerQuery planning and delegates work to the classes that do the actual planning of
QueryGraphs and EventHorizons
 */
case class PlanSingleQuery(planPart: (PlannerQuery, LogicalPlanningContext, Option[LogicalPlan]) => LogicalPlan = planPart,
                           planEventHorizon: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = PlanEventHorizon,
                           expressionRewriterFactory: (LogicalPlanningContext => Rewriter) = ExpressionRewriterFactory,
                           planWithTail: LogicalPlanningFunction2[LogicalPlan, Option[PlannerQuery], LogicalPlan] = PlanWithTail(),
                           planUpdates: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = PlanUpdates) extends LogicalPlanningFunction1[PlannerQuery, LogicalPlan] {

  override def apply(in: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val partPlan = countStorePlanner(in).getOrElse(planPart(in, context, None))

    //we cannot force eagerness for merge queries
    val alwaysEager = context.config.updateStrategy.alwaysEager && !in.updateGraph.containsMerge
    val planWithEffect =
      if (alwaysEager || conflicts(partPlan, in))
        context.logicalPlanProducer.planEager(partPlan)
      else partPlan
    val planWithUpdates = planUpdates(in, planWithEffect)(context)
    val planWithUpdatesAndEffects =
      if (alwaysEager || in.updateGraph.mergeNodeDeleteOverlap)
        context.logicalPlanProducer.planEager(planWithUpdates)
      else planWithUpdates

    val projectedPlan = planEventHorizon(in, planWithUpdatesAndEffects)
    val projectedContext = context.recurse(projectedPlan)
    val expressionRewriter = expressionRewriterFactory(projectedContext)
    val completePlan = projectedPlan.endoRewrite(expressionRewriter)

    val finalPlan = planWithTail(completePlan, in.tail)(projectedContext)
    verifyBestPlan(finalPlan, in)
  }

  /*
   * The first reading leaf node is always stable. However for every preceding leaf node
   * we must make sure there are no updates in this planner query that will match any of the reads.
   * If so we must make that read a RepeatableRead.
   */
  private def conflicts(plan: LogicalPlan, plannerQuery: PlannerQuery): Boolean = {
    if (plannerQuery.updateGraph.isEmpty) false
    else {
      val leaves = plan.leaves.collect {
        case n: NodeLogicalLeafPlan => n.idName
      }
      //if we have unsafe rels we need to check relation overlap and delete
      //overlap immediately
      (hasUnsafeRelationships(plannerQuery) &&
        (plannerQuery.updateGraph.createRelationshipOverlap(plannerQuery.queryGraph) ||
          plannerQuery.updateGraph.deleteOverlap(plannerQuery.queryGraph) ||
          plannerQuery.updateGraph.setPropertyOverlap(plannerQuery.queryGraph))
      ) ||
        //otherwise only do checks if we have more that one leaf
        leaves.size > 1 && leaves.drop(1).exists(
          nodeOverlap(_, plannerQuery, leaves.head) ||
            plannerQuery.updateGraph.createRelationshipOverlap(plannerQuery.queryGraph) ||
            plannerQuery.updateGraph.setLabelOverlap(plannerQuery.queryGraph) ||
            plannerQuery.updateGraph.setPropertyOverlap(plannerQuery.queryGraph) ||
            plannerQuery.updateGraph.deleteOverlap(plannerQuery.queryGraph))
    }
  }

  /*
   * Check if the labels of the node with the provided IdName overlaps
   * with the labels updated in this query. This may cause the read to affected
   * by the writes.
   */
  private def nodeOverlap(start: IdName, plannerQuery: PlannerQuery, stableId: IdName): Boolean = {
    val startLabels = plannerQuery.queryGraph.allKnownLabelsOnNode(start).toSet
    val writeLabels = plannerQuery.updateGraph.createLabels
    val createProperties = plannerQuery.updateGraph.createNodeProperties
    val removeLabels = plannerQuery.updateGraph.labelsToRemoveForNode(start)
    val startProperties = plannerQuery.queryGraph.allKnownPropertiesOnIdentifier(start).map(_.propertyKey)

    plannerQuery.updatesNodes && (
      ( ((startLabels.isEmpty && startProperties.isEmpty) && plannerQuery.createsNodes) || //MATCH () CREATE (...)?
        (startLabels intersect writeLabels).nonEmpty) || //MATCH (:A) CREATE (:A)?
        startProperties.exists(createProperties.overlaps) || //MATCH ({prop:42}) CREATE ({prop:...})

        //MATCH (n:A), (m:B) REMOVE (n:B)
        removeLabels.exists(l => {
          //it is ok to have overlap on the stable id
          val labels = plannerQuery.queryGraph.patternNodes.filterNot(i => i == stableId || i == start )
            .flatMap(plannerQuery.queryGraph.allKnownLabelsOnNode)
          labels(l)
        })
      )
  }

  /*
   * Unsafe relationships are whatever that may cause unstable
   * iterators when expanding. The unsafe cases are:
   * - (a)-[r]-(b) (undirected)
   * - (a)-[r1]->(b)-[r2]->(c) (multi step)
   * - (a)-[r*]->(b) (variable length)
   */
  private def hasUnsafeRelationships(pq: PlannerQuery) = {
    val allPatterns = pq.queryGraph.allPatternRelationships
    allPatterns.size > 1 ||
      allPatterns.exists(r => r.dir == SemanticDirection.BOTH || !r.length.isSimple)
  }
}
