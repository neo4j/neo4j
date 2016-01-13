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
    query.updateGraph.mutatingPatterns.foldLeft(plan)((plan, pattern) => planUpdate(query, plan, pattern))

  private def planUpdate(query: PlannerQuery, source: LogicalPlan, pattern: MutatingPattern)(implicit context: LogicalPlanningContext): LogicalPlan = pattern match {
    //CREATE ()
    case p: CreateNodePattern => context.logicalPlanProducer.planCreateNode(source, p)
    //CREATE (a)-[:R]->(b)
    case p: CreateRelationshipPattern => context.logicalPlanProducer.planCreateRelationship(source, p)
    //MERGE ()
    case p: MergeNodePattern =>
      val mergePlan = planMerge(query, source, Seq(p.createNodePattern), Seq.empty, p.matchGraph, p.onCreate, p.onMatch)
      //we have to force the plan to solve what we actually solve
      val solved = context.logicalPlanProducer.estimatePlannerQuery(
        source.solved.amendUpdateGraph(u => u.addMutatingPatterns(p)))
      mergePlan.updateSolved(solved)

    //MERGE (a)-[:T]->(b)
    case p: MergeRelationshipPattern =>
      val mergePlan = planMerge(query, source, p.createNodePatterns, p.createRelPatterns, p.matchGraph, p.onCreate, p.onMatch)
      //we have to force the plan to solve what we actually solve
      val solved = context.logicalPlanProducer.estimatePlannerQuery(
        source.solved.amendUpdateGraph(u => u.addMutatingPatterns(p)))
      mergePlan.updateSolved(solved)

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
      val delete = p.expression match {
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

      if (context.config.updateStrategy.alwaysEager || query.updateGraph.mergeDeleteOverlap)
        context.logicalPlanProducer.planEager(delete)
      else delete
  }

  /*
   * Merges either match or create. It is planned as following.
   *
   *                     |
   *                anti-cond-apply
   *                  /     \
   *                 /    on-create
   *                /        \
   *               /   merge-create-part
   *         cond-apply
   *             /   \
   *          apply  on-match
   *          /   \
   *         /  optional
   *        /      \
   *  (source)  merge-read-part
   *
   * Note also that merge uses a special leaf planner to enforce the correct behavior
   * when having uniqueness constraints.
   */
  private def planMerge(query: PlannerQuery, source: LogicalPlan, createNodePatterns: Seq[CreateNodePattern],
                            createRelationshipPatterns: Seq[CreateRelationshipPattern], matchGraph: QueryGraph,
                            onCreate: Seq[SetMutatingPattern], onMatch: Seq[SetMutatingPattern])(implicit context: LogicalPlanningContext) = {
    //use a special unique-index leaf planner
    val leafPlanners = PriorityLeafPlannerList(LeafPlannerList(mergeUniqueIndexSeekLeafPlanner),
      context.config.leafPlanners)
    val innerContext: LogicalPlanningContext =
      context.recurse(source).copy(config = context.config.withLeafPlanners(leafPlanners))

    //        apply
    //        /   \
    //       /  optional
    //      /       \
    //(source)  merge-read-part
    val matchPart = innerContext.strategy.plan(matchGraph)(innerContext)
    val producer = innerContext.logicalPlanProducer
    val rhs = producer.planOptional(matchPart, source.availableSymbols)(innerContext)
    val apply = producer.planApply(source, rhs)(innerContext)

    //           cond-apply
    //             /   \
    //          apply  on-match
    val ids = createNodePatterns.map(_.nodeName) ++ createRelationshipPatterns.map(_.relName)

    val conditionalApply = if (onMatch.nonEmpty) {
      val onMatchPlan = onMatch.foldLeft[LogicalPlan](producer.planSingleRow()) {
        case (src, current) => planUpdate(query, src, current)
      }
      producer.planConditionalApply(apply, onMatchPlan, ids)(innerContext)
    } else apply

    //       anti-cond-apply
    //         /     \
    //        /    on-create
    //       /        \
    //      /   merge-create-part
    //cond-apply
    val createNodes = createNodePatterns.foldLeft(context.logicalPlanProducer.planSingleRow():LogicalPlan){
      case (acc, current) => producer.planMergeCreateNode(acc, current)
    }
    val createRels = createRelationshipPatterns.foldLeft(createNodes) {
      case (acc, current) => producer.planMergeCreateRelationship(acc, current)
    }

    val onCreatePlan = onCreate.foldLeft(createRels) {
      case (src, current) => planUpdate(query, src, current)
    }

    producer.planAntiConditionalApply(
      conditionalApply, onCreatePlan, ids)(innerContext)
  }
}
