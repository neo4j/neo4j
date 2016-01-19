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
    query.queryGraph.mutatingPatterns.foldLeft(plan) {
      case (acc, pattern) => planUpdate(query, acc, pattern)
    }

  private def planUpdate(query: PlannerQuery, source: LogicalPlan, pattern: MutatingPattern)(implicit context: LogicalPlanningContext): LogicalPlan = {
    def solvedMergePattern = {
      val solved = PlannerQuery.asMergePlannerQuery(
        source.solved
          .amendQueryGraph(q => q.addPatternNodes(q.optionalMatches.head.patternNodes.toSeq: _*)
            .addPatternRelationships(q.optionalMatches.head.patternRelationships.toSeq)
            .withOptionalMatches(Seq.empty)
            .withArgumentIds(query.queryGraph.argumentIds)
            .withSelections(query.queryGraph.selections)
            .addMutatingPatterns(pattern)
          ))
      val solvedWithEstimate = context.logicalPlanProducer.estimatePlannerQuery(solved)
      solvedWithEstimate
    }

    pattern match {
      //FOREACH
      case foreach: ForeachPattern => {
        val innerLeaf = context.logicalPlanProducer.planArgumentRow(Set.empty, Set.empty, source.availableSymbols + foreach.variable)
        val innerUpdatePlan = this.apply(foreach.innerUpdates, innerLeaf)
        val foreachPlan = context.logicalPlanProducer.planForeach(source, innerUpdatePlan, foreach)
        foreachPlan
      }
      //CREATE ()
      case p: CreateNodePattern => context.logicalPlanProducer.planCreateNode(source, p)
      //CREATE (a)-[:R]->(b)
      case p: CreateRelationshipPattern => context.logicalPlanProducer.planCreateRelationship(source, p)
      //MERGE ()
      case p: MergeNodePattern =>
        val mergePlan = planMergeWritePart(query, source, p.matchGraph, Seq(p.createNodePattern), Seq.empty, p.onCreate, p.onMatch)
        //we have to force the plan to solve what we actually solve
        mergePlan.updateSolved(solvedMergePattern)
      //MERGE (a)-[:T]->(b)
      case p: MergeRelationshipPattern =>
        val mergePlan = planMergeWritePart(query, source, p.matchGraph, p.createNodePatterns, p.createRelPatterns, p.onCreate, p.onMatch)
        //we have to force the plan to solve what we actually solve
        mergePlan.updateSolved(solvedMergePattern)
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
          case PathExpression(e) =>
            context.logicalPlanProducer.planDeletePath(source, p)
          //DELETE users[{i}]
          case ContainerIndex(Variable(n), indexExpr) if context.semanticTable.isNodeCollection(n) =>
            context.logicalPlanProducer.planDeleteNode(source, p)
          //DELETE rels[{i}]
          case ContainerIndex(Variable(r), indexExpr) if context.semanticTable.isRelationshipCollection(r) =>
            context.logicalPlanProducer.planDeleteRelationship(source, p)
          //DELETE expr
          case expr =>
            context.logicalPlanProducer.planDeleteExpression(source, p)
        }

        delete
    }
  }

  /*
   * Merges either match or create. It is planned as following.
   *
   *
   *      apply
   *      /    \
   *   (source) \
   *             \
   *          anti-cond-apply
   *              /     \
   *             /    on-create
   *            /         \
   *           /    merge-create-part
   *          /
   *       cond-apply
   *        /     \
   *    optional  on-match
   *      /
   *  merge-read-part
   *
   * Note also that merge uses a special leaf planner to enforce the correct behavior
   * when having uniqueness constraints.
   */
  def planMergeReadPart(source: LogicalPlan, matchGraph: QueryGraph)
                       (implicit context: LogicalPlanningContext) = {
    //Merge needs to make sure that found nodes have all the expected properties, so we use AssertSame operators here
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
    if (matchPart.solved.queryGraph != matchGraph)
      throw new CantHandleQueryException(s"The planner was unable to successfully plan the MERGE read: ${matchPart.solved.queryGraph} not equal to $matchGraph")

    val producer = innerContext.logicalPlanProducer
    val rhs = producer.planOptional(matchPart, matchGraph.argumentIds)(innerContext)
    val apply = producer.planApply(source, rhs)(innerContext)
    apply
  }

  def planMergeWritePart(query: PlannerQuery, source: LogicalPlan, matchGraph: QueryGraph, createNodePatterns: Seq[CreateNodePattern],
                         createRelationshipPatterns: Seq[CreateRelationshipPattern],
                         onCreate: Seq[SetMutatingPattern], onMatch: Seq[SetMutatingPattern])
                        (implicit context: LogicalPlanningContext): LogicalPlan = {

    val producer = context.logicalPlanProducer

    //           cond-apply
    //             /   \
    //        (source)  on-match
    val ids = createNodePatterns.map(_.nodeName) ++ createRelationshipPatterns.map(_.relName)

    val conditionalApply = if (onMatch.nonEmpty) {
      val onMatchPlan = onMatch.foldLeft[LogicalPlan](producer.planSingleRow()) {
        case (src, current) => planUpdate(query, src, current)
      }
      producer.planConditionalApply(source, onMatchPlan, ids)(context)
    } else source

    //       anti-cond-apply
    //         /     \
    //        /    on-create
    //       /        \
    //      /   merge-create-part
    //cond-apply
    val createNodes = createNodePatterns.foldLeft(context.logicalPlanProducer.planQueryArgumentRow(matchGraph): LogicalPlan){
      case (acc, current) => producer.planMergeCreateNode(acc, current)
    }
    val createRels = createRelationshipPatterns.foldLeft(createNodes) {
      case (acc, current) => producer.planMergeCreateRelationship(acc, current)
    }

    val onCreatePlan = onCreate.foldLeft(createRels) {
      case (src, current) => planUpdate(query, src, current)
    }

    val antiConditionalApply = producer.planAntiConditionalApply(conditionalApply, onCreatePlan, ids)(context)

    antiConditionalApply
  }
}
