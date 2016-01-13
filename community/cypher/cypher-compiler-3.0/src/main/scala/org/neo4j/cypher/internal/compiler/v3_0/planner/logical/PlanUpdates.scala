/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.mergeUniqueIndexSeekLeafPlanner
import org.neo4j.cypher.internal.frontend.v3_0.ast.{ContainerIndex, PathExpression, Variable}

/*
 * This coordinates PlannerQuery planning of updates.
 */
case object PlanUpdates
  extends LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] {

  override def apply(query: PlannerQuery, plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan =
    query.updateGraph.mutatingPatterns.foldLeft(plan)((plan, pattern) => pattern match {
      case p: MergeNodePattern => planMergeNode(query, plan, p)
      case _ if query.queryGraph.nonEmpty =>
        val argument = context.logicalPlanProducer.planQueryArgumentRow(query.queryGraph)
        context.logicalPlanProducer.planApply(plan, planUpdate(query, argument, pattern))
      case _ =>
        planUpdate(query, plan, pattern)
    })

  private def planUpdate(query: PlannerQuery, source: LogicalPlan, pattern: MutatingPattern)(implicit context: LogicalPlanningContext): LogicalPlan = pattern match {
    //CREATE ()
    case p: CreateNodePattern => context.logicalPlanProducer.planCreateNode(source, p)
    //CREATE (a)-[:R]->(b)
    case p: CreateRelationshipPattern => context.logicalPlanProducer.planCreateRelationship(source, p)
    //MERGE ()
//    case p: MergeNodePattern => planMergeNode(query, source, p)
    //SET n:Foo:Bar
    case pattern: SetLabelPattern => context.logicalPlanProducer.planSetLabel(source, pattern)
    //SET n.prop = 42
    case pattern: SetNodePropertyPattern =>
      context.logicalPlanProducer.planSetNodeProperty(source, pattern)
    //SET r.prop = 42
    case pattern: SetRelationshipPropertyPattern =>
      context.logicalPlanProducer.planSetRelationshipProperty(source, pattern)
    //SET n.prop += {}
    case pattern: SetNodePropertiesFromMapPattern =>
      context.logicalPlanProducer.planSetNodePropertiesFromMap(source, pattern)
    //SET r.prop = 42
    case pattern: SetRelationshipPropertiesFromMapPattern =>
      context.logicalPlanProducer.planSetRelationshipPropertiesFromMap(source, pattern)
    //REMOVE n:Foo:Bar
    case pattern: RemoveLabelPattern => context.logicalPlanProducer.planRemoveLabel(source, pattern)
    //DELETE a
    case p: DeleteExpression =>
      p.expression match {
        //DELETE user
        case Variable(n) if context.semanticTable.isNode(n) =>
          context.logicalPlanProducer.planDeleteNode(source, p)
        //DELETE rel
        case Variable(r) if context.semanticTable.isRelationship(r) =>
          context.logicalPlanProducer.planDeleteRelationship(source, p)
        //DELETE path
        case PathExpression(e)  =>
          context.logicalPlanProducer.planDeletePath(source, p)
        //DELETE users[{i}]
        case ContainerIndex(Variable(n),indexExpr) if context.semanticTable.isNodeCollection(n) =>
          context.logicalPlanProducer.planDeleteNode(source, p)
        //DELETE rels[{i}]
        case ContainerIndex(Variable(r),indexExpr) if context.semanticTable.isRelationshipCollection(r) =>
          context.logicalPlanProducer.planDeleteRelationship(source, p)
        //DELETE expr
        case expr =>
          context.logicalPlanProducer.planDeleteExpression(source, p)
      }
  }

  /*
   * Merges either match or create. It is planned as following.
   *
   *                     |
   *                anti-cond-apply
   *                  /     \
   *                 /    on-create
   *                /         \
   *               /    merge-create-part
   *         cond-apply
   *             /   \
   *           apply  on-match
   *           /   \
   *          /  optional
   *         /       \
   *   (source)   merge-read-part
   *
   * Note also that merge uses a special leaf planner to enforce the correct behavior
   * when having uniqueness constraints.
   */
  private def planMergeNode(query: PlannerQuery, source: LogicalPlan, merge: MergeNodePattern)(implicit context: LogicalPlanningContext) = {
    //use a special unique-index leaf planner
    val leafPlanners = PriorityLeafPlannerList(LeafPlannerList(mergeUniqueIndexSeekLeafPlanner),
      context.config.leafPlanners)
    val innerContext: LogicalPlanningContext =
      context.recurse(source).copy(config = context.config.withLeafPlanners(leafPlanners))

    // If we have a DELETE and MERGE in the update graph, we need an extra eager
    val producer = innerContext.logicalPlanProducer
    val sourceWithEager = if (context.config.updateStrategy.alwaysEager || query.updateGraph.deleteOverlapWithMergeNodeInSelf) {
      producer.planEager(source)
    } else source

    //        apply
    //        /   \
    //       /  optional
    //      /       \
    //(source)  merge-read-part
    val matchPart = innerContext.strategy.plan(merge.matchGraph)(innerContext)
    if (matchPart.solved.queryGraph != merge.matchGraph)
      throw new CantHandleQueryException(s"The planner was unable to successfully plan the MERGE read: ${matchPart.solved.queryGraph} not equal to ${merge.matchGraph}")
    val rhs = producer.planOptional(matchPart, sourceWithEager.availableSymbols)(innerContext)
    val apply = producer.planApply(sourceWithEager, rhs)(innerContext)

    //           cond-apply
    //             /   \
    //          apply  on-match
    val conditionalApply = if (merge.onMatch.nonEmpty) {
      val onMatch = merge.onMatch.foldLeft[LogicalPlan](producer.planSingleRow()) {
        case (src, current) => planUpdate(query, src, current)
      }
      producer.planConditionalApply(apply, onMatch, merge.createNodePattern.nodeName)(innerContext)
    } else apply

    //       anti-cond-apply
    //         /     \
    //        /    on-create
    //       /        \
    //      /   merge-create-part
    //cond-apply
    val create = producer.planMergeCreateNode(context.logicalPlanProducer.planSingleRow(),
      merge.createNodePattern)
    val onCreate = merge.onCreate.foldLeft(create) {
      case (src, current) => planUpdate(query, src, current)
    }
    //we have to force the plan to solve what we actually solve
    val solved = producer.estimatePlannerQuery(
      sourceWithEager.solved.amendUpdateGraph(u => u.addMutatingPatterns(merge)))

    producer.planAntiConditionalApply(
      conditionalApply, onCreate, merge.createNodePattern.nodeName)(innerContext).updateSolved(solved)
  }
}
