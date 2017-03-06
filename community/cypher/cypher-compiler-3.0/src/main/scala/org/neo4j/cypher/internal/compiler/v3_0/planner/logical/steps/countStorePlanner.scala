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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_0.pipes.LazyTypes
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v3_0.ast._

case object countStorePlanner {

  def apply(query: PlannerQuery)(implicit context: LogicalPlanningContext): Option[LogicalPlan] = {
    implicit val semanticTable = context.semanticTable
    query.horizon match {
      case AggregatingQueryProjection(groupingKeys, aggregatingExpressions, shuffle)
        if groupingKeys.isEmpty && aggregatingExpressions.size == 1 =>
        val (columnName, exp) = aggregatingExpressions.head
        val countStorePlan = checkForValidQueryGraph(query, columnName, exp)
        countStorePlan.map(p => projection(p, groupingKeys))

      case _ => None
    }
  }

  private def checkForValidQueryGraph(query: PlannerQuery, columnName: String, exp: Expression)
                                     (implicit context: LogicalPlanningContext): Option[LogicalPlan] = query.queryGraph match {
    case QueryGraph(patternRelationships, patternNodes, argumentIds, selections, Seq(), hints, shortestPathPatterns, _)
      if hints.isEmpty && shortestPathPatterns.isEmpty && query.queryGraph.readOnly =>
      checkForValidAggregations(query, columnName, exp, patternRelationships, patternNodes, argumentIds, selections)
    case _ => None
  }

  private def checkForValidAggregations(query: PlannerQuery, columnName: String, exp: Expression,
                                        patternRelationships: Set[PatternRelationship], patternNodes: Set[IdName],
                                        argumentIds: Set[IdName], selections: Selections)(implicit context: LogicalPlanningContext): Option[LogicalPlan] =
    exp match {
      case // COUNT(<id>)
        func@FunctionInvocation(_, false, Vector(Variable(variableName))) if func.function contains functions.Count =>
        trySolveNodeAggregation(query, columnName, Some(variableName), patternRelationships, patternNodes, argumentIds, selections)

      case // COUNT(*)
        CountStar() =>
        trySolveNodeAggregation(query, columnName, None, patternRelationships, patternNodes, argumentIds, selections)

      case // COUNT(n.prop)
        func@FunctionInvocation(_, false, Vector(Property(Variable(variableName), PropertyKeyName(propKeyName))))
        if func.function contains functions.Count =>
        val labelCheck: Option[LabelName] => (Option[LogicalPlan] => Option[LogicalPlan]) = {
            case None => _ => None
            case Some(LabelName(labelName)) => (plan: Option[LogicalPlan]) => plan.filter(_ => context.planContext.hasPropertyExistenceConstraint(labelName, propKeyName))
          }
        trySolveNodeAggregation(query, columnName, None, patternRelationships, patternNodes, argumentIds, selections, labelCheck)

      case _ => None
    }

  private def trySolveNodeAggregation(query: PlannerQuery, columnName: String, variableName: Option[String],
                                      patternRelationships: Set[PatternRelationship], patternNodes: Set[IdName], argumentIds: Set[IdName],
                                      selections: Selections,
                                      // This function is used when the aggregation needs a specific label to exist,
                                      // for constraint checking
                                      labelCheck: Option[LabelName] => (Option[LogicalPlan] => Option[LogicalPlan]) = _ => identity)
                                     (implicit context: LogicalPlanningContext): Option[LogicalPlan] = {
    if (patternNodes.size == 1 &&
      patternRelationships.isEmpty &&
      variableName.forall(_ == patternNodes.head.name) &&
      noWrongPredicates(Set(patternNodes.head), selections)) { // MATCH (n), MATCH (n:A)
      val label = findLabel(patternNodes.head, selections)
      val lpp = context.logicalPlanProducer
      val aggregation1 = lpp.planCountStoreNodeAggregation(query, IdName(columnName), label, argumentIds)(context)
      labelCheck(label)(Some(aggregation1))
    } else if (patternRelationships.size == 1 && notLoop(patternRelationships.head)) { // MATCH ()-[r]->(), MATCH ()-[r:X]->(), MATCH ()-[r:X|Y]->()
      labelCheck(None)(
        trySolveRelationshipAggregation(query, columnName, variableName, patternRelationships, argumentIds, selections)
      )
    } else None
  }

  // the counts store counts loops twice
  private def notLoop(r: PatternRelationship) = r.nodes._1 != r.nodes._2

  private def trySolveRelationshipAggregation(query: PlannerQuery, columnName: String, variableName: Option[String],
                                              patternRelationships: Set[PatternRelationship], argumentIds: Set[IdName],
                                              selections: Selections)(implicit context: LogicalPlanningContext): Option[RelationshipCountFromCountStore] = {
    patternRelationships.head match {

      case PatternRelationship(relId, (startNodeId, endNodeId), direction, types, SimplePatternLength)
        if variableName.forall(_ == relId.name) && noWrongPredicates(Set(startNodeId, endNodeId), selections) =>

        def planRelAggr(fromLabel: Option[LabelName], toLabel: Option[LabelName]) =
          Some(context.logicalPlanProducer.planCountStoreRelationshipAggregation(query, IdName(columnName), fromLabel, LazyTypes(types.map(_.name)), toLabel, argumentIds)(context))

        (findLabel(startNodeId, selections), direction, findLabel(endNodeId, selections)) match {
          case (None,       OUTGOING, None) => planRelAggr(None, None)
          case (None,       INCOMING, None) => planRelAggr(None, None)
          case (None,       OUTGOING, endLabel) => planRelAggr(None, endLabel)
          case (startLabel, OUTGOING, None) => planRelAggr(startLabel, None)
          case (None,       INCOMING, endLabel) => planRelAggr(endLabel, None)
          case (startLabel, INCOMING, None) => planRelAggr(None, startLabel)
          case _ => None
        }

      case _ => None
    }
  }

  def noWrongPredicates(nodeIds: Set[IdName], selections: Selections): Boolean = {
    val (labelPredicates, other) = selections.predicates.partition {
      case Predicate(nIds, h: HasLabels) if nIds.forall(nodeIds.contains) && h.labels.size == 1 => true
      case _ => false
    }
    labelPredicates.size <= 1 && other.isEmpty
  }

  def findLabel(nodeId: IdName, selections: Selections): Option[LabelName] = selections.predicates.collectFirst {
    case Predicate(nIds, h: HasLabels) if nIds == Set(nodeId) && h.labels.size == 1 => h.labels.head
  }
}
