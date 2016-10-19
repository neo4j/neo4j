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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.planner._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.mergeUniqueIndexSeekLeafPlanner
import org.neo4j.cypher.internal.frontend.v3_2.ast.{ContainerIndex, PathExpression, Variable}

/*
 * This coordinates PlannerQuery planning of updates.
 */
case object PlanUpdates
  extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {

  override def apply(query: PlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean)(implicit context: LogicalPlanningContext): LogicalPlan = {
    // Eagerness pass 1 -- does previously planned reads conflict with future writes?
    val plan = if (firstPlannerQuery)
      Eagerness.headReadWriteEagerize(in, query)
    else
      //// NOTE: tailReadWriteEagerizeRecursive is done after updates, below
      Eagerness.tailReadWriteEagerizeNonRecursive(in, query)

    val updatePlan = query.queryGraph.mutatingPatterns.foldLeft(plan) {
      case (acc, pattern) => planUpdate(query, acc, pattern, firstPlannerQuery)
    }

    if (firstPlannerQuery)
      Eagerness.headWriteReadEagerize(updatePlan, query)
    else {
      Eagerness.tailWriteReadEagerize(Eagerness.tailReadWriteEagerizeRecursive(updatePlan, query), query)
    }
  }

  private def planUpdate(query: PlannerQuery, source: LogicalPlan, pattern: MutatingPattern, first: Boolean)
                          (implicit context: LogicalPlanningContext): LogicalPlan = {

    def planAllUpdatesRecursively(query: PlannerQuery, plan: LogicalPlan): LogicalPlan = {
      query.allPlannerQueries.foldLeft((plan, true)) {
        case ((accPlan, innerFirst), plannerQuery) => (this.apply(plannerQuery, accPlan, innerFirst), false)
      }._1
    }

    pattern match {
      //FOREACH
      case foreach: ForeachPattern =>
        val innerLeaf = context.logicalPlanProducer
          .planArgumentRow(Set.empty, Set.empty, source.availableSymbols + foreach.variable)
        val innerUpdatePlan = planAllUpdatesRecursively(foreach.innerUpdates, innerLeaf)
        context.logicalPlanProducer.planForeachApply(source, innerUpdatePlan, foreach)

      //CREATE ()
      case p: CreateNodePattern => context.logicalPlanProducer.planCreateNode(source, p)
      //CREATE (a)-[:R]->(b)
      case p: CreateRelationshipPattern => context.logicalPlanProducer.planCreateRelationship(source, p)
      //MERGE ()
      case p: MergeNodePattern =>
        val mergePlan = planMerge(query, source, p.matchGraph, Seq(p.createNodePattern), Seq.empty, p.onCreate,
          p.onMatch, first)
        //we have to force the plan to solve what we actually solve
        val solved = context.logicalPlanProducer.estimatePlannerQuery(
          source.solved.amendQueryGraph(u => u.addMutatingPatterns(p)))
        mergePlan.updateSolved(solved)

      //MERGE (a)-[:T]->(b)
      case p: MergeRelationshipPattern =>
        val mergePlan = planMerge(query, source, p.matchGraph, p.createNodePatterns, p.createRelPatterns, p.onCreate,
          p.onMatch, first)
        //we have to force the plan to solve what we actually solve
        val solved = context.logicalPlanProducer.estimatePlannerQuery(
          source.solved.amendQueryGraph(u => u.addMutatingPatterns(p)))
        mergePlan.updateSolved(solved)
      //SET n:Foo:Bar
      case pattern: SetLabelPattern => context.logicalPlanProducer.planSetLabel(source, pattern)
      //SET n.prop = 42
      case pattern: SetNodePropertyPattern =>
        context.logicalPlanProducer.planSetNodeProperty(source, pattern)
      //SET r.prop = 42
      case pattern: SetRelationshipPropertyPattern =>
        context.logicalPlanProducer.planSetRelationshipProperty(source, pattern)
      case pattern: SetPropertyPattern =>
        context.logicalPlanProducer.planSetProperty(source, pattern)
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
   * Merges either matches or creates. It is planned as following:
   *
   *
   *            antiCondApply
   *              /     \
   *             /    onCreate
   *            /         \
   *           /      mergeCreatePart
   *          /
   *       cond-apply
   *        /     \
   *    apply  onMatch
   *    /    \
   * source optional
   *           \
   *         mergeReadPart
   *
   * Note also that merge uses a special leaf planner to enforce the correct behavior
   * when having uniqueness constraints, and unnestApply will remove a lot of the extra Applies
   */
  private def planMerge(query: PlannerQuery, source: LogicalPlan, matchGraph: QueryGraph, createNodePatterns: Seq[CreateNodePattern],
                createRelationshipPatterns: Seq[CreateRelationshipPattern], onCreatePatterns: Seq[SetMutatingPattern],
                onMatchPatterns: Seq[SetMutatingPattern], first: Boolean)(implicit context: LogicalPlanningContext): LogicalPlan = {

    val producer = context.logicalPlanProducer

    //Merge needs to make sure that found nodes have all the expected properties, so we use AssertSame operators here
    val leafPlanners = PriorityLeafPlannerList(LeafPlannerList(Seq(mergeUniqueIndexSeekLeafPlanner)),
      context.config.leafPlanners)

    val innerContext: LogicalPlanningContext =
      context.recurse(source).copy(config = context.config.withLeafPlanners(leafPlanners))

    //        apply
    //        /   \
    //       /  optional
    //      /       \
    // source  mergeReadPart
    val mergeReadPart = innerContext.strategy.plan(matchGraph)(innerContext, None)
    if (mergeReadPart.solved.queryGraph != matchGraph)
      throw new CantHandleQueryException(s"The planner was unable to successfully plan the MERGE read:\n${mergeReadPart.solved.queryGraph}\n not equal to \n$matchGraph")

    val rhs = producer.planOptional(mergeReadPart, matchGraph.argumentIds)(innerContext)
    val apply = producer.planApply(source, rhs)(innerContext)

    //            condApply
    //             /   \
    //          apply  onMatch
    val ids = createNodePatterns.map(_.nodeName) ++ createRelationshipPatterns.map(_.relName)

    val condApply = if (onMatchPatterns.nonEmpty) {
      val qgWithAllNeededArguments = matchGraph.addArgumentIds(matchGraph.allCoveredIds.toIndexedSeq)
      val onMatch = onMatchPatterns.foldLeft[LogicalPlan](producer.planQueryArgumentRow(qgWithAllNeededArguments)) {
        case (src, current) => planUpdate(query, src, current, first)
      }
      producer.planConditionalApply(apply, onMatch, ids)(innerContext)
    } else apply

    //       antiCondApply
    //         /     \
    //        /    onCreate
    //       /         \
    //      /     mergeCreatePart
    // condApply
    val createNodes = createNodePatterns.foldLeft(producer.planQueryArgumentRow(matchGraph): LogicalPlan){
      case (acc, current) => producer.planMergeCreateNode(acc, current)
    }
    val mergeCreatePart = createRelationshipPatterns.foldLeft(createNodes) {
      case (acc, current) => producer.planMergeCreateRelationship(acc, current)
    }

    val onCreate = onCreatePatterns.foldLeft(mergeCreatePart) {
      case (src, current) => planUpdate(query, src, current, first)
    }

    val antiCondApply = producer.planAntiConditionalApply(condApply, onCreate, ids)(innerContext)

    antiCondApply
  }
}
