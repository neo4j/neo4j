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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.{DeleteExpression => _, _}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, ast}
import org.neo4j.cypher.internal.ir.v3_2._
import org.neo4j.cypher.internal.ir.v3_2.exception.CantHandleQueryException

/*
 * This coordinates PlannerQuery planning of updates.
 */
case object PlanUpdates
  extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {

  override def apply(query: PlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean)
                    (implicit context: LogicalPlanningContext): LogicalPlan = {
    // Eagerness pass 1 -- does previously planned reads conflict with future writes?
    val plan = if (firstPlannerQuery)
      Eagerness.headReadWriteEagerize(in, query)
    else
    //// NOTE: tailReadWriteEagerizeRecursive is done after updates, below
      Eagerness.tailReadWriteEagerizeNonRecursive(in, query)

    val updatePlan = query.queryGraph.mutatingPatterns.foldLeft(plan) {
      case (acc, pattern) => planUpdate(acc, pattern, firstPlannerQuery)
    }

    if (firstPlannerQuery)
      Eagerness.headWriteReadEagerize(updatePlan, query)
    else {
      Eagerness.tailWriteReadEagerize(Eagerness.tailReadWriteEagerizeRecursive(updatePlan, query), query)
    }
  }

  private def planUpdate(source: LogicalPlan, pattern: MutatingPattern, first: Boolean)
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
        val mergePlan = planMerge(source, p.matchGraph, Seq(p.createNodePattern), Seq.empty, p.onCreate,
          p.onMatch, first)
        //we have to force the plan to solve what we actually solve
        val solved = context.logicalPlanProducer.estimatePlannerQuery(
          source.solved.amendQueryGraph(u => u.addMutatingPatterns(p)))
        mergePlan.updateSolved(solved)

      //MERGE (a)-[:T]->(b)
      case p: MergeRelationshipPattern =>
        val mergePlan = planMerge(source, p.matchGraph, p.createNodePatterns, p.createRelPatterns, p.onCreate,
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
          case ast.Variable(n) if context.semanticTable.isNode(n) =>
            context.logicalPlanProducer.planDeleteNode(source, p)
          //DELETE rel
          case ast.Variable(r) if context.semanticTable.isRelationship(r) =>
            context.logicalPlanProducer.planDeleteRelationship(source, p)
          //DELETE path
          case ast.PathExpression(e) =>
            context.logicalPlanProducer.planDeletePath(source, p)
          //DELETE users[{i}]
          case ast.ContainerIndex(ast.Variable(n), indexExpr) if context.semanticTable.isNodeCollection(n) =>
            context.logicalPlanProducer.planDeleteNode(source, p)
          //DELETE rels[{i}]
          case ast.ContainerIndex(ast.Variable(r), indexExpr) if context.semanticTable.isRelationshipCollection(r) =>
            context.logicalPlanProducer.planDeleteRelationship(source, p)
          //DELETE expr
          case expr =>
            context.logicalPlanProducer.planDeleteExpression(source, p)
        }
        delete
    }
  }

  /*
   * MERGE either matches or creates. It is planned as following:
   *                                 cond-apply
   *                                 x IS NULL
   *                                /         \
   *                           cond-apply   onCreate
   *                          x IS NOT NULL    \
   *                          /          \    CREATE
   *                       cond-apply   onMatch
   *                       x IS NULL \
   *                       /       apply
   *                    apply      /   \
   *                    /    \  lock-X optional
   *                lock-S optional       \
   *                  /       \          MATCH
   *               source    MATCH         \
   *                            \          LockNodes(X)
   *                            Arg         \
   *                                         Arg
   */
  def planMerge(source: LogicalPlan, matchGraph: QueryGraph, createNodePatterns: Seq[CreateNodePattern],
                createRelationshipPatterns: Seq[CreateRelationshipPattern], onCreatePatterns: Seq[SetMutatingPattern],
                onMatchPatterns: Seq[SetMutatingPattern], first: Boolean)(implicit context: LogicalPlanningContext): LogicalPlan = {

    val producer: LogicalPlanProducer = context.logicalPlanProducer
    val ids: Seq[IdName] = createNodePatterns.map(_.nodeName) ++ createRelationshipPatterns.map(_.relName)

    val mergeMatch = mergeMatchPart(source, matchGraph, producer, createNodePatterns, createRelationshipPatterns, context, ids)

    //            condApply
    //             /   \
    //          apply  onMatch
    val condApply = if (onMatchPatterns.nonEmpty) {
      val qgWithAllNeededArguments = matchGraph.addArgumentIds(matchGraph.allCoveredIds.toIndexedSeq)
      val onMatch = onMatchPatterns.foldLeft[LogicalPlan](producer.planQueryArgumentRow(qgWithAllNeededArguments)) {
        case (src, current) => planUpdate(src, current, first)
      }

      producer.planConditionalApply(mergeMatch, onMatch, checkForNull(ids, negated = true))(context)
    } else mergeMatch

    //       antiCondApply
    //         /     \
    //        /    onCreate
    //       /         \
    //      /     mergeCreatePart
    // condApply
    val createNodes = createNodePatterns.foldLeft(producer.planQueryArgumentRow(matchGraph): LogicalPlan) {
      case (acc, current) => producer.planMergeCreateNode(acc, current)
    }
    val mergeCreatePart = createRelationshipPatterns.foldLeft(createNodes) {
      case (acc, current) => producer.planMergeCreateRelationship(acc, current)
    }

    val onCreate = onCreatePatterns.foldLeft(mergeCreatePart) {
      case (src, current) => planUpdate(src, current, first)
    }
    val antiCondApply = producer.planConditionalApply(condApply, onCreate, checkForNull(ids, negated = false))(context)

    antiCondApply
  }

  private val pos = InputPosition.NONE

  /*
     If negated, produces a predicate such as
     WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND D IS NOT NULL

     If not negated,
     WHERE a IS NULL OR b IS NULL OR c IS NULL or D IS NULL
     */
  private def checkForNull(ids: Seq[IdName], negated: Boolean): Expression = {
    val predicates: Seq[Expression] = ids map {
      case IdName(key) =>
        val isNull = ast.IsNull(ast.Variable(key)(pos))(pos)
        if (negated)
          ast.Not(isNull)(pos)
        else
          isNull
    }

    val predicate = if (predicates.size == 1)
      predicates.head
    else {
      if (negated)
        ast.Ands(predicates.toSet)(pos)
      else
        ast.Ors(predicates.toSet)(pos)
    }
    predicate
  }

  private def mergeMatchPart(in: LogicalPlan,
                             matchGraph: QueryGraph,
                             producer: LogicalPlanProducer,
                             createNodePatterns: Seq[CreateNodePattern],
                             createRelationshipPatterns: Seq[CreateRelationshipPattern],
                             context: LogicalPlanningContext,
                             ids: Seq[IdName]): LogicalPlan = {
    def mergeRead(ctx: LogicalPlanningContext) = {
      val mergeReadPart = ctx.strategy.plan(matchGraph)(ctx)
      if (mergeReadPart.solved.queryGraph != matchGraph)
        throw new CantHandleQueryException(s"The planner was unable to successfully plan the MERGE read:\n${mergeReadPart.solved.queryGraph}\n not equal to \n$matchGraph")
      producer.planOptional(mergeReadPart, matchGraph.argumentIds)(ctx)
    }

    val mergeLocks: Seq[LockDescription] = createLockingDescriptions(matchGraph)
    // Grab merge locks if we need to
    val source = if (mergeLocks.nonEmpty)
      context.logicalPlanProducer.planMergeLock(in, mergeLocks, Shared)
    else
      in

    //        apply
    //        /   \
    //       /  optional
    //      /       \
    // source  mergeReadPart
    //                \
    //                arg
    val matchOrNull = producer.planApply(source, mergeRead(context))(context)
    val nullPredicate = checkForNull(ids, negated = false)

    // If we are MERGEing on relationships, we need to lock nodes before matching again. Otherwise, we are done
    val nodesToLock = matchGraph.patternNodes intersect matchGraph.argumentIds

    if (nodesToLock.nonEmpty || mergeLocks.nonEmpty) {
      val arg = context.logicalPlanProducer.planArgumentRowFrom(matchOrNull)(context)
      val lhsOfApply = if (mergeLocks.nonEmpty)
      //       CondApply (check if null)
      //          /   \
      //         /  merge-lock(X)
      //        /       \
      //   source   single-row
        grabExclusiveLocks(producer, context, mergeLocks, arg, nullPredicate)
      else
        arg

      val rhsOfApply = grabNodeLocks(producer, context, mergeRead, nullPredicate, nodesToLock)

      val applyPlan = context.logicalPlanProducer.planApply(lhsOfApply, rhsOfApply)(context)
      producer.planConditionalApply(matchOrNull, applyPlan, nullPredicate)(context)

    } else {
      matchOrNull
    }
  }


  private def grabNodeLocks(producer: LogicalPlanProducer, context: LogicalPlanningContext,
                            mergeRead: LogicalPlanningContext => LogicalPlan, nullPredicate: Expression,
                            nodesToLock: Set[IdName]) = {
    def addLockToPlan(plan: LogicalPlan): LogicalPlan = {
      val lockThese = nodesToLock intersect plan.availableSymbols
      if (lockThese.nonEmpty)
        LockNodes(plan, lockThese)(plan.solved)
      else
        plan
    }

    val lockingContext = context.copy(leafPlanUpdater = addLockToPlan)

    //        CondApply
    //        /   \
    //       /  optional
    //      /       \
    // source  mergeReadPart
    //                \
    //             lock-nodes
    //                  \
    //                  leaf
    mergeRead(lockingContext)
  }

  private def grabExclusiveLocks(producer: LogicalPlanProducer, context: LogicalPlanningContext,
                                 mergeLocks: Seq[LockDescription], source: LogicalPlan, nullPredicate: Expression) = {
    val singleRow = context.logicalPlanProducer.planArgumentRowFrom(source)(context)
    context.logicalPlanProducer.planMergeLock(singleRow, mergeLocks, Exclusive)
  }

  def createLockingDescriptions(qg: QueryGraph): Seq[LockDescription] = {
    val nodeIds = qg.patternNodes -- qg.argumentIds
    val nodesWithLabels = nodeIds.flatMap(id => qg.selections.labelsOnNode(id).map(lbl => id -> lbl))
    val lockDescriptions = nodesWithLabels flatMap {
      case (nodeId, label) =>
        val propValues = qg.selections.predicates.collect {
          case Predicate(_, Equals(Property(Variable(x), propertyKeyName), exp)) if x == nodeId.name =>
            Seq(propertyKeyName -> exp)
          case Predicate(_, In(Property(Variable(x), propertyKeyName), values: ListLiteral)) if x == nodeId.name =>
            values.expressions.map(e => propertyKeyName -> e)
        }.flatten
        if (propValues.isEmpty)
          None
        else
          Some(LockDescription(label, propValues.toSeq))
    }

    lockDescriptions.toSeq
  }
}
