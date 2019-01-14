/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.{LogicalPlanProducer, mergeUniqueIndexSeekLeafPlanner}
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.v3_4.expressions.{ContainerIndex, PathExpression, Variable}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

/*
 * This coordinates PlannerQuery planning of updates.
 */
case object PlanUpdates
  extends ((PlannerQuery, LogicalPlan, Boolean, LogicalPlanningContext, Solveds, Cardinalities) => (LogicalPlan, LogicalPlanningContext)) {

  private def computePlan(plan: LogicalPlan, query: PlannerQuery, firstPlannerQuery: Boolean, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities) = {
    var updatePlan = plan
    var ctx = context
    val iterator = query.queryGraph.mutatingPatterns.iterator
    while(iterator.hasNext) {
      updatePlan = planUpdate(updatePlan, iterator.next(), firstPlannerQuery, ctx, solveds, cardinalities)
    }
    (updatePlan, ctx)
  }

  override def apply(query: PlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): (LogicalPlan, LogicalPlanningContext) = {
    // Eagerness pass 1 -- does previously planned reads conflict with future writes?
    val plan = if (firstPlannerQuery)
      Eagerness.headReadWriteEagerize(in, query, context)
    else
    //// NOTE: tailReadWriteEagerizeRecursive is done after updates, below
      Eagerness.tailReadWriteEagerizeNonRecursive(in, query, context)

    val (updatePlan, finalContext) = computePlan(plan, query, firstPlannerQuery, context, solveds, cardinalities)

    val lp = if (firstPlannerQuery)
      Eagerness.headWriteReadEagerize(updatePlan, query, context)
    else {
      Eagerness.tailWriteReadEagerize(Eagerness.tailReadWriteEagerizeRecursive(updatePlan, query, context), query, context)
    }
    (lp, context)
  }

  private def planUpdate(source: LogicalPlan, pattern: MutatingPattern, first: Boolean, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): LogicalPlan = {

    def planAllUpdatesRecursively(query: PlannerQuery, plan: LogicalPlan): LogicalPlan = {
      query.allPlannerQueries.foldLeft((plan, true, context)) {
        case ((accPlan, innerFirst, accCtx), plannerQuery) =>
          val (newPlan,newCtx) = this.apply(plannerQuery, accPlan, innerFirst, context, solveds, cardinalities)
          (newPlan, false, newCtx)
      }._1
    }

    pattern match {
      //FOREACH
      case foreach: ForeachPattern =>
        val innerLeaf = context.logicalPlanProducer
          .planArgument(Set.empty, Set.empty, source.availableSymbols + foreach.variable, context)
        val innerUpdatePlan = planAllUpdatesRecursively(foreach.innerUpdates, innerLeaf)
        context.logicalPlanProducer.planForeachApply(source, innerUpdatePlan, foreach, context)

      //CREATE ()
      case p: CreateNodePattern => context.logicalPlanProducer.planCreateNode(source, p, context)

      //CREATE (a)-[:R]->(b)
      case p: CreateRelationshipPattern => context.logicalPlanProducer.planCreateRelationship(source, p, context)

      //MERGE ()
      case p: MergeNodePattern =>
        planMerge(source, p.matchGraph, Seq(p.createNodePattern), Seq.empty, p.onCreate,
          p.onMatch, first, context, solveds, cardinalities, p)

      //MERGE (a)-[:T]->(b)
      case p: MergeRelationshipPattern =>
        planMerge(source, p.matchGraph, p.createNodePatterns, p.createRelPatterns, p.onCreate,
          p.onMatch, first, context, solveds, cardinalities, p)

      //SET n:Foo:Bar
      case pattern: SetLabelPattern => context.logicalPlanProducer.planSetLabel(source, pattern, context)

      //SET n.prop = 42
      case pattern: SetNodePropertyPattern =>
        context.logicalPlanProducer.planSetNodeProperty(source, pattern, context)

      //SET r.prop = 42
      case pattern: SetRelationshipPropertyPattern =>
        context.logicalPlanProducer.planSetRelationshipProperty(source, pattern, context)

      //SET x.prop = 42
      case pattern: SetPropertyPattern =>
        context.logicalPlanProducer.planSetProperty(source, pattern, context)

      //SET n.prop += {}
      case pattern: SetNodePropertiesFromMapPattern =>
        context.logicalPlanProducer.planSetNodePropertiesFromMap(source, pattern, context)

      //SET r.prop += {}
      case pattern: SetRelationshipPropertiesFromMapPattern =>
        context.logicalPlanProducer.planSetRelationshipPropertiesFromMap(source, pattern, context)

      //REMOVE n:Foo:Bar
      case pattern: RemoveLabelPattern => context.logicalPlanProducer.planRemoveLabel(source, pattern, context)

      //DELETE a
      case p: DeleteExpression =>
        val delete = p.expression match {
          //DELETE user
          case Variable(n) if context.semanticTable.isNode(n) =>
            context.logicalPlanProducer.planDeleteNode(source, p, context)

          //DELETE rel
          case Variable(r) if context.semanticTable.isRelationship(r) =>
            context.logicalPlanProducer.planDeleteRelationship(source, p, context)

          //DELETE path
          case PathExpression(e) =>
            context.logicalPlanProducer.planDeletePath(source, p, context)

          //DELETE users[{i}]
          case ContainerIndex(Variable(n), indexExpr) if context.semanticTable.isNodeCollection(n) =>
            context.logicalPlanProducer.planDeleteNode(source, p, context)

          //DELETE rels[{i}]
          case ContainerIndex(Variable(r), indexExpr) if context.semanticTable.isRelationshipCollection(r) =>
            context.logicalPlanProducer.planDeleteRelationship(source, p, context)

          //DELETE expr
          case expr =>
            context.logicalPlanProducer.planDeleteExpression(source, p, context)
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
  def planMerge(source: LogicalPlan, matchGraph: QueryGraph, createNodePatterns: Seq[CreateNodePattern],
                createRelationshipPatterns: Seq[CreateRelationshipPattern], onCreatePatterns: Seq[SetMutatingPattern],
                onMatchPatterns: Seq[SetMutatingPattern], first: Boolean, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities,
                solvedMutatingPattern: MutatingPattern): LogicalPlan = {

    val producer: LogicalPlanProducer = context.logicalPlanProducer

    //Merge needs to make sure that found nodes have all the expected properties, so we use AssertSame operators here
    val leafPlannerList = LeafPlannerList(IndexedSeq(mergeUniqueIndexSeekLeafPlanner))
    val leafPlanners = PriorityLeafPlannerList(leafPlannerList, context.config.leafPlanners)

    val innerContext: LogicalPlanningContext =
      context.withUpdatedCardinalityInformation(source, solveds, cardinalities).copy(config = context.config.withLeafPlanners(leafPlanners))

    val ids: Seq[String] = createNodePatterns.map(_.nodeName) ++ createRelationshipPatterns.map(_.relName)

    val mergeMatch = mergeMatchPart(source, matchGraph, producer, createNodePatterns, createRelationshipPatterns, innerContext, solveds, cardinalities, ids)

    //            condApply
    //             /   \
    //          apply  onMatch
    val condApply = if (onMatchPatterns.nonEmpty) {
      val qgWithAllNeededArguments = matchGraph.addArgumentIds(matchGraph.allCoveredIds.toIndexedSeq)
      val onMatch = onMatchPatterns.foldLeft[LogicalPlan](producer.planQueryArgument(qgWithAllNeededArguments, context)) {
        case (src, current) => planUpdate(src, current, first, context, solveds, cardinalities)
      }
      producer.planConditionalApply(mergeMatch, onMatch, ids, innerContext)
    } else mergeMatch

    //       antiCondApply
    //         /     \
    //        /    onCreate
    //       /         \
    //      /     mergeCreatePart
    // condApply
    val createNodes = createNodePatterns.foldLeft(producer.planQueryArgument(matchGraph, context): LogicalPlan) {
      case (acc, current) => producer.planMergeCreateNode(acc, current, context)
    }
    val mergeCreatePart = createRelationshipPatterns.foldLeft(createNodes) {
      case (acc, current) => producer.planMergeCreateRelationship(acc, current, context)
    }

    val onCreate = onCreatePatterns.foldLeft(mergeCreatePart) {
      case (src, current) => planUpdate(src, current, first, context, solveds, cardinalities)
    }

    val solved = solveds.get(source.id).amendQueryGraph(u => u.addMutatingPatterns(solvedMutatingPattern))
    val antiCondApply = producer.planAntiConditionalApply(condApply, onCreate, ids, innerContext, Some(solved))

    antiCondApply
  }

  private def mergeMatchPart(source: LogicalPlan,
                             matchGraph: QueryGraph,
                             producer: LogicalPlanProducer,
                             createNodePatterns: Seq[CreateNodePattern],
                             createRelationshipPatterns: Seq[CreateRelationshipPattern],
                             context: LogicalPlanningContext,
                             solveds: Solveds,
                             cardinalities: Cardinalities,
                             ids: Seq[String]): LogicalPlan = {
    def mergeRead(ctx: LogicalPlanningContext) = {
      val mergeReadPart = ctx.strategy.plan(matchGraph, ctx, solveds, cardinalities)
      if (solveds.get(mergeReadPart.id).queryGraph != matchGraph)
        throw new InternalException(s"The planner was unable to successfully plan the MERGE read:\n${solveds.get(mergeReadPart.id).queryGraph}\n not equal to \n$matchGraph")
      val activeReadPart = producer.planActiveRead(mergeReadPart, ctx)
      producer.planOptional(activeReadPart, matchGraph.argumentIds, ctx)
    }

    //        apply
    //        /   \
    //       /  optional
    //      /       \
    // source  mergeReadPart
    //                \
    //                arg
    val matchOrNull = producer.planApply(source, mergeRead(context), context)

    // If we are MERGEing on relationships, we need to lock nodes before matching again. Otherwise, we are done
    val nodesToLock = matchGraph.patternNodes intersect matchGraph.argumentIds

    if (nodesToLock.nonEmpty) {

      def addLockToPlan(plan: LogicalPlan): LogicalPlan = {
        val lockThese = nodesToLock intersect plan.availableSymbols
        if (lockThese.nonEmpty)
          producer.planLock(plan, lockThese, context)
        else
          plan
      }

      val lockingContext = context.copy(leafPlanUpdater = addLockToPlan)

      //        antiCondApply
      //        /   \
      //       /  optional
      //      /       \
      // source  mergeReadPart
      //                \
      //               lock
      //                  \
      //                  leaf
      val ifMissingLockAndMatchAgain = mergeRead(lockingContext)
      producer.planAntiConditionalApply(matchOrNull, ifMissingLockAndMatchAgain, ids, context)
    } else
      matchOrNull
  }
}
