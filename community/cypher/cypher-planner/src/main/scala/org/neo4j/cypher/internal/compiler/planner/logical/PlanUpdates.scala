/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.planner.logical.steps.{LogicalPlanProducer, mergeUniqueIndexSeekLeafPlanner}
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.expressions.{ContainerIndex, PathExpression, Variable}
import org.neo4j.exceptions.InternalException

/*
 * This coordinates PlannerQuery planning of updates.
 */
case object PlanUpdates extends UpdatesPlanner {

  private def computePlan(plan: LogicalPlan, query: SinglePlannerQuery, firstPlannerQuery: Boolean, context: LogicalPlanningContext) = {
    var updatePlan = plan
    val iterator = query.queryGraph.mutatingPatterns.iterator
    while(iterator.hasNext) {
      updatePlan = planUpdate(updatePlan, iterator.next(), firstPlannerQuery, query.interestingOrder, context)
    }
    (updatePlan, context)
  }

  override def apply(query: SinglePlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean, context: LogicalPlanningContext): (LogicalPlan, LogicalPlanningContext) = {
    // Eagerness pass 1 -- does previously planned reads conflict with future writes?
    val plan = if (firstPlannerQuery)
      Eagerness.headReadWriteEagerize(in, query, context)
    else
    //// NOTE: tailReadWriteEagerizeRecursive is done after updates, below
      Eagerness.tailReadWriteEagerizeNonRecursive(in, query, context)

    val (updatePlan, finalContext) = computePlan(plan, query, firstPlannerQuery, context)

    val lp = if (firstPlannerQuery)
      Eagerness.headWriteReadEagerize(updatePlan, query, context)
    else {
      Eagerness.tailWriteReadEagerize(Eagerness.tailReadWriteEagerizeRecursive(updatePlan, query, context), query, context)
    }
    (lp, context)
  }

  private def planUpdate(source: LogicalPlan, pattern: MutatingPattern, first: Boolean, interestingOrder: InterestingOrder, context: LogicalPlanningContext) = {

    def planAllUpdatesRecursively(query: SinglePlannerQuery, plan: LogicalPlan): LogicalPlan = {
      query.allPlannerQueries.foldLeft((plan, true, context)) {
        case ((accPlan, innerFirst, accCtx), plannerQuery) =>
          val (newPlan,newCtx) = this.apply(plannerQuery, accPlan, innerFirst, context)
          (newPlan, false, newCtx)
      }._1
    }

    pattern match {
      //FOREACH
      case foreach: ForeachPattern =>
        val innerLeaf = context.logicalPlanProducer.planArgument(Set.empty, Set.empty, source.availableSymbols + foreach.variable, context)
        val innerUpdatePlan = planAllUpdatesRecursively(foreach.innerUpdates, innerLeaf)
        context.logicalPlanProducer.planForeachApply(source, innerUpdatePlan, foreach, context, interestingOrder, foreach.expression)

      //CREATE ()
      //CREATE (a)-[:R]->(b)
      //CREATE (), (a)-[:R]->(b), (x)-[:X]->(y)-[:Y]->(z {prop:2})
      case p: CreatePattern => context.logicalPlanProducer.planCreate(source, p, interestingOrder, context)

      //MERGE ()
      case p: MergeNodePattern =>
        planMerge(source, p.matchGraph, Seq(p.createNode), Seq.empty, p.onCreate, p.onMatch, first, interestingOrder, context, p)

      //MERGE (a)-[:T]->(b)
      case p: MergeRelationshipPattern =>
        planMerge(source, p.matchGraph, p.createNodes, p.createRelationships, p.onCreate, p.onMatch, first, interestingOrder, context, p)

      //SET n:Foo:Bar
      case pattern: SetLabelPattern => context.logicalPlanProducer.planSetLabel(source, pattern, context)

      //SET n.prop = 42
      case pattern:SetNodePropertyPattern =>
        context.logicalPlanProducer.planSetNodeProperty(source, pattern, interestingOrder, context)

      //SET r.prop = 42
      case pattern:SetRelationshipPropertyPattern =>
        context.logicalPlanProducer.planSetRelationshipProperty(source, pattern, interestingOrder, context)

      //SET x.prop = 42
      case pattern:SetPropertyPattern =>
        context.logicalPlanProducer.planSetProperty(source, pattern, interestingOrder, context)

      //SET n += {p1: ..., p2: ...}
      case pattern:SetNodePropertiesFromMapPattern =>
        context.logicalPlanProducer.planSetNodePropertiesFromMap(source, pattern, interestingOrder, context)

      //SET r += {p1: ..., p2: ...}
      case pattern:SetRelationshipPropertiesFromMapPattern =>
        context.logicalPlanProducer.planSetRelationshipPropertiesFromMap(source, pattern, interestingOrder, context)

      //SET x += {p1: ..., p2: ...}
      case pattern:SetPropertiesFromMapPattern =>
        context.logicalPlanProducer.planSetPropertiesFromMap(source, pattern,  interestingOrder, context)

      //REMOVE n:Foo:Bar
      case pattern: RemoveLabelPattern => context.logicalPlanProducer.planRemoveLabel(source, pattern, context)

      //DELETE a
      case p: DeleteExpression =>
        val delete = p.expression match {
          //DELETE user
          case Variable(n) if context.semanticTable.isNode(n) =>
            context.logicalPlanProducer.planDeleteNode(source, p, interestingOrder, context)

          //DELETE rel
          case Variable(r) if context.semanticTable.isRelationship(r) =>
            context.logicalPlanProducer.planDeleteRelationship(source, p, interestingOrder, context)

          //DELETE path
          case PathExpression(_) =>
            context.logicalPlanProducer.planDeletePath(source, p, context)

          //DELETE users[{i}]
          case ContainerIndex(Variable(n), _) if context.semanticTable.isNodeCollection(n) =>
            context.logicalPlanProducer.planDeleteNode(source, p, interestingOrder, context)

          //DELETE rels[{i}]
          case ContainerIndex(Variable(r), _) if context.semanticTable.isRelationshipCollection(r) =>
            context.logicalPlanProducer.planDeleteRelationship(source, p, interestingOrder, context)

          //DELETE expr
          case _ =>
            context.logicalPlanProducer.planDeleteExpression(source, p, interestingOrder, context)
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
  def planMerge(source: LogicalPlan,
                matchGraph: QueryGraph,
                createNodePatterns: Seq[CreateNode],
                createRelationshipPatterns: Seq[CreateRelationship],
                onCreatePatterns: Seq[SetMutatingPattern],
                onMatchPatterns: Seq[SetMutatingPattern],
                first: Boolean,
                interestingOrder: InterestingOrder,
                context: LogicalPlanningContext,
                solvedMutatingPattern: MutatingPattern): LogicalPlan = {

    val producer: LogicalPlanProducer = context.logicalPlanProducer

    //Merge needs to make sure that found nodes have all the expected properties, so we use AssertSame operators here
    val leafPlannerList = LeafPlannerList(IndexedSeq(mergeUniqueIndexSeekLeafPlanner))
    val leafPlanners = PriorityLeafPlannerList(leafPlannerList, context.config.leafPlanners)

    val innerContext: LogicalPlanningContext =
      context.withUpdatedCardinalityInformation(source).copy(config = context.config.withLeafPlanners(leafPlanners))

    val ids: Seq[String] = createNodePatterns.map(_.idName) ++ createRelationshipPatterns.map(_.idName)

    val mergeMatch = mergeMatchPart(source, matchGraph, producer, createNodePatterns, createRelationshipPatterns, interestingOrder, innerContext, ids)

    //            condApply
    //             /   \
    //          apply  onMatch
    val condApply = if (onMatchPatterns.nonEmpty) {
      val qgWithAllNeededArguments = matchGraph.addArgumentIds(matchGraph.allCoveredIds.toIndexedSeq)
      val onMatch = onMatchPatterns.foldLeft[LogicalPlan](producer.planQueryArgument(qgWithAllNeededArguments, context)) {
        case (src, current) => planUpdate(src, current, first, interestingOrder, context)
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
      case (acc, current) => producer.planMergeCreateNode(acc, current, interestingOrder, context)
    }
    val mergeCreatePart = createRelationshipPatterns.foldLeft(createNodes) {
      case (acc, current) => producer.planMergeCreateRelationship(acc, current, interestingOrder, context)
    }

    val onCreate = onCreatePatterns.foldLeft(mergeCreatePart) {
      case (src, current) => planUpdate(src, current, first, interestingOrder, context)
    }

    val solved = context.planningAttributes.solveds.get(source.id).asSinglePlannerQuery.amendQueryGraph(u => u.addMutatingPatterns(solvedMutatingPattern))
    val antiCondApply = producer.planAntiConditionalApply(condApply, onCreate, ids, innerContext, Some(solved))

    antiCondApply
  }

  private def mergeMatchPart(source: LogicalPlan,
                             matchGraph: QueryGraph,
                             producer: LogicalPlanProducer,
                             createNodePatterns: Seq[CreateNode],
                             createRelationshipPatterns: Seq[CreateRelationship],
                             interestingOrder: InterestingOrder,
                             context: LogicalPlanningContext, ids: Seq[String]) = {
    def mergeRead(ctx: LogicalPlanningContext) = {
      val mergeReadPart = ctx.strategy.plan(matchGraph, interestingOrder, ctx)
      if (context.planningAttributes.solveds.get(mergeReadPart.id).asSinglePlannerQuery.queryGraph != matchGraph)
        throw new InternalException(s"The planner was unable to successfully plan the MERGE read:\n${context.planningAttributes.solveds.get(mergeReadPart.id).asSinglePlannerQuery.queryGraph}\n not equal to \n$matchGraph")
      producer.planOptional(mergeReadPart, matchGraph.argumentIds, ctx)
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
      val lockingContext = context.withAddedLeafPlanUpdater(AddLockToPlan(nodesToLock, producer, context))

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

  case class AddLockToPlan(nodesToLock: Set[String], producer: LogicalPlanProducer, context: LogicalPlanningContext) extends LeafPlanUpdater {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      val lockThese = nodesToLock intersect plan.availableSymbols
      if (lockThese.nonEmpty)
        producer.planLock(plan, lockThese, context)
      else
        plan
    }
  }
}
