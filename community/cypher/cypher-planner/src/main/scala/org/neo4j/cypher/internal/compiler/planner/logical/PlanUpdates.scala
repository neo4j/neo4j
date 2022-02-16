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

import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.planner.logical.steps.mergeUniqueIndexSeekLeafPlanner
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.ForeachPattern
import org.neo4j.cypher.internal.ir.MergeNodePattern
import org.neo4j.cypher.internal.ir.MergeRelationshipPattern
import org.neo4j.cypher.internal.ir.MutatingPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.exceptions.InternalException

/*
 * This coordinates PlannerQuery planning of updates.
 */
case object PlanUpdates extends UpdatesPlanner {

  private def computePlan(plan: LogicalPlan, query: SinglePlannerQuery, eagerAnalyzer: EagerAnalyzer, context: LogicalPlanningContext) = {
    val orderForPlanning = InterestingOrderConfig(query.interestingOrder)

    case class Acc(updatePlan: LogicalPlan, patternsToPlan: IndexedSeq[MutatingPattern])

    val Acc(updatePlan, _) = query.queryGraph.mutatingPatterns.foldLeft(Acc(plan, query.queryGraph.mutatingPatterns)) {
      case (Acc(updatePlan, pattersToPlan), nextPatternToPlan) =>
        val nextUpdatePlan = planUpdate(updatePlan, nextPatternToPlan, orderForPlanning, context)
        val remainingPatternToPlan = pattersToPlan.tail

        // This query is constructed such that the current write is placed on its own QueryGraph
        // and all remaining writes are placed in the next query graph.
        val queryToCheckForConflicts = RegularSinglePlannerQuery(
          queryGraph = QueryGraph(mutatingPatterns = IndexedSeq(nextPatternToPlan)),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(mutatingPatterns = remainingPatternToPlan)
          ))
        )

        val eagerizedNextUpdatePlan = eagerAnalyzer.tailWriteReadEagerize(
          eagerAnalyzer.tailReadWriteEagerizeRecursive(nextUpdatePlan, queryToCheckForConflicts),
          queryToCheckForConflicts
        )
        Acc(eagerizedNextUpdatePlan, remainingPatternToPlan)
    }
    updatePlan
  }

  override def plan(query: SinglePlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean, context: LogicalPlanningContext): LogicalPlan = {
    val eagerAnalyzer = new EagerAnalyzer(context)
    // Eagerness pass 1 -- does previously planned reads conflict with future writes?
    val plan = if (firstPlannerQuery)
      eagerAnalyzer.headReadWriteEagerize(in, query)
    else
    //// NOTE: tailReadWriteEagerizeRecursive is done after updates, below
      eagerAnalyzer.tailReadWriteEagerizeNonRecursive(in, query)

    val updatePlan = computePlan(plan, query, eagerAnalyzer, context)

    if (firstPlannerQuery)
      eagerAnalyzer.headWriteReadEagerize(updatePlan, query)
    else {
      eagerAnalyzer.tailWriteReadEagerize(eagerAnalyzer.tailReadWriteEagerizeRecursive(updatePlan, query), query)
    }
  }

  private def planUpdate(source: LogicalPlan,
                         pattern: MutatingPattern,
                         interestingOrderConfig: InterestingOrderConfig,
                         context: LogicalPlanningContext) = {

    def planAllUpdatesRecursively(query: SinglePlannerQuery, plan: LogicalPlan): LogicalPlan = {
      query.allPlannerQueries.foldLeft((plan, true)) {
        case ((accPlan, innerFirst), plannerQuery) =>
          val newPlan = this.plan(plannerQuery, accPlan, innerFirst, context)
          (newPlan, false)
      }._1
    }

    pattern match {
      //FOREACH
      case foreach: ForeachPattern =>

        val allPatterns = foreach.innerUpdates.allPlannerQueries.flatMap(_.queryGraph.mutatingPatterns)
        val sideEffects = allPatterns.collect {
          case s: SimpleMutatingPattern => s
        }
        if (allPatterns.length == sideEffects.length) {
          context.logicalPlanProducer.planForeach(source, foreach, context, foreach.expression, sideEffects)
        } else {
          val innerLeaf = context.logicalPlanProducer.planArgument(Set.empty, Set.empty, source.availableSymbols + foreach.variable, context)
          val innerUpdatePlan = planAllUpdatesRecursively(foreach.innerUpdates, innerLeaf)
          context.logicalPlanProducer.planForeachApply(source, innerUpdatePlan, foreach, context, foreach.expression)
        }

      //CREATE ()
      //CREATE (a)-[:R]->(b)
      //CREATE (), (a)-[:R]->(b), (x)-[:X]->(y)-[:Y]->(z {prop:2})
      case p: CreatePattern => context.logicalPlanProducer.planCreate(source, p, context)

      //MERGE ()
      case p: MergeNodePattern =>
        planMerge(source, p.matchGraph, Seq(p.createNode), Seq.empty, p.onCreate, p.onMatch, interestingOrderConfig, context)

      //MERGE (a)-[:T]->(b)
      case p: MergeRelationshipPattern =>
        planMerge(source, p.matchGraph, p.createNodes, p.createRelationships, p.onCreate, p.onMatch, interestingOrderConfig, context)

      //SET n:Foo:Bar
      case pattern: SetLabelPattern => context.logicalPlanProducer.planSetLabel(source, pattern, context)

      //SET n.prop = 42
      case pattern:SetNodePropertyPattern =>
        context.logicalPlanProducer.planSetNodeProperty(source, pattern, context)

      //SET r.prop = 42
      case pattern:SetRelationshipPropertyPattern =>
        context.logicalPlanProducer.planSetRelationshipProperty(source, pattern, context)

      //SET x.prop = 42
      case pattern:SetPropertyPattern =>
        context.logicalPlanProducer.planSetProperty(source, pattern, context)

      //SET n += {p1: ..., p2: ...}
      case pattern:SetNodePropertiesFromMapPattern =>
        context.logicalPlanProducer.planSetNodePropertiesFromMap(source, pattern, context)

      //SET r += {p1: ..., p2: ...}
      case pattern:SetRelationshipPropertiesFromMapPattern =>
        context.logicalPlanProducer.planSetRelationshipPropertiesFromMap(source, pattern, context)

      //SET x += {p1: ..., p2: ...}
      case pattern:SetPropertiesFromMapPattern =>
        context.logicalPlanProducer.planSetPropertiesFromMap(source, pattern, context)

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
          case PathExpression(_) =>
            context.logicalPlanProducer.planDeletePath(source, p, context)

          //DELETE users[{i}]
          case ContainerIndex(Variable(n), _) if context.semanticTable.isNodeCollection(n) =>
            context.logicalPlanProducer.planDeleteNode(source, p, context)

          //DELETE rels[{i}]
          case ContainerIndex(Variable(r), _) if context.semanticTable.isRelationshipCollection(r) =>
            context.logicalPlanProducer.planDeleteRelationship(source, p, context)

          //DELETE expr
          case _ =>
            context.logicalPlanProducer.planDeleteExpression(source, p, context)
        }
        delete
    }
  }

  /**
   * Merge either matches the pattern or creates the pattern.
   *
   * For a query like:
   *
   * `MATCH (a:A) MERGE (b:B) ON MATCH SET b.prop = 1 ON CREATE SET b.prop = 2`
   *
   * we will plan it like
   *
   * {{{
   *        apply
   *         /\
   * scan(a)   merge(create(b), set(b.prop=1), set(b.prop=2))
   *             |
   *           scan(b)
   * }}}
   *
   * When merging on a relationship we take node locks to prevent the creation of multiple relationships,
   * so for something like `MATCH (a:A) MERGE (a)-[r:R]->(b)`
   *
   * we will plan something like:
   *
   * {{{
   *        apply
   *         /\
   * scan(a)  merge(create(b), create(r))
   *             |
   *          either
   *            /\
   *      expand expand
   *               \
   *              lock(a)
   * }}}
   */
  def planMerge(source: LogicalPlan,
                matchGraph: QueryGraph,
                createNodePatterns: Seq[CreateNode],
                createRelationshipPatterns: Seq[CreateRelationship],
                onCreatePatterns: Seq[SetMutatingPattern],
                onMatchPatterns: Seq[SetMutatingPattern],
                interestingOrderConfig: InterestingOrderConfig,
                context: LogicalPlanningContext): LogicalPlan = {
    def mergeRead(ctx: LogicalPlanningContext) = {
      val mergeReadPart = ctx.strategy.plan(matchGraph, interestingOrderConfig, ctx).result
      if (context.planningAttributes.solveds.get(mergeReadPart.id).asSinglePlannerQuery.queryGraph != matchGraph)
        throw new InternalException(s"The planner was unable to successfully plan the MERGE read:\n${context.planningAttributes.solveds.get(mergeReadPart.id).asSinglePlannerQuery.queryGraph}\n not equal to \n$matchGraph")
      mergeReadPart
    }
    val producer: LogicalPlanProducer = context.logicalPlanProducer

    //Merge needs to make sure that found nodes have all the expected properties, so we use AssertSame operators here
    val leafPlannerList = LeafPlannerList(IndexedSeq(mergeUniqueIndexSeekLeafPlanner))
    val leafPlanners = PriorityLeafPlannerList(leafPlannerList, context.config.leafPlanners)

    val innerContext: LogicalPlanningContext =
      context.withUpdatedLabelInfo(source).copy(config = context.config.withLeafPlanners(leafPlanners))
    val read =  mergeRead(innerContext)
    // If we are MERGEing on relationships, we need to lock nodes before matching again. Otherwise, we are done
    val nodesToLock = matchGraph.patternNodes intersect matchGraph.argumentIds
    producer.planApply(source, producer.planMerge(read, createNodePatterns, createRelationshipPatterns, onMatchPatterns, onCreatePatterns, nodesToLock, context), context)
  }
}
