/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.opencypher.v9_0.expressions.SemanticDirection.{INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan

case object countStorePlanner {

  def apply(query: PlannerQuery, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): Option[LogicalPlan] = {
    implicit val semanticTable = context.semanticTable
    query.horizon match {
      case AggregatingQueryProjection(groupingKeys, aggregatingExpressions, _)
        if groupingKeys.isEmpty && aggregatingExpressions.size == 1 =>
        val (columnName, exp) = aggregatingExpressions.head
        val countStorePlan = checkForValidQueryGraph(query, columnName, exp, context)
        countStorePlan.map(p => projection(p, groupingKeys, context, solveds, cardinalities))

      case _ => None
    }
  }

  private def checkForValidQueryGraph(query: PlannerQuery, columnName: String, exp: Expression, context: LogicalPlanningContext): Option[LogicalPlan] = query.queryGraph match {
    case QueryGraph(patternRelationships, patternNodes, argumentIds, selections, Seq(), hints, shortestPathPatterns, _)
      if hints.isEmpty && shortestPathPatterns.isEmpty && query.queryGraph.readOnly =>
      checkForValidAggregations(query, columnName, exp, patternRelationships, patternNodes, argumentIds, selections, context)
    case _ => None
  }

  private def checkForValidAggregations(query: PlannerQuery, columnName: String, exp: Expression,
                                        patternRelationships: Set[PatternRelationship], patternNodes: Set[String],
                                        argumentIds: Set[String], selections: Selections, context: LogicalPlanningContext): Option[LogicalPlan] =
    exp match {
      case // COUNT(<id>)
        func@FunctionInvocation(_, _, false, Vector(Variable(variableName))) if func.function == functions.Count =>
        trySolveNodeAggregation(query, columnName, Some(variableName), patternRelationships, patternNodes, argumentIds, selections, context)

      case // COUNT(*)
        CountStar() =>
        trySolveNodeAggregation(query, columnName, None, patternRelationships, patternNodes, argumentIds, selections, context)

      case // COUNT(n.prop)
        func@FunctionInvocation(_, _, false, Vector(Property(Variable(variableName), PropertyKeyName(propKeyName))))
        if func.function == functions.Count =>
        val labelCheck: Option[LabelName] => (Option[LogicalPlan] => Option[LogicalPlan]) = {
            case None => _ => None
            case Some(LabelName(labelName)) => (plan: Option[LogicalPlan]) => plan.filter(_ => context.planContext.hasPropertyExistenceConstraint(labelName, propKeyName))
          }
        trySolveNodeAggregation(query, columnName, None, patternRelationships, patternNodes, argumentIds, selections, context, labelCheck)

      case _ => None
    }

  private def trySolveNodeAggregation(query: PlannerQuery, columnName: String, variableName: Option[String],
                                      patternRelationships: Set[PatternRelationship], patternNodes: Set[String], argumentIds: Set[String],
                                      selections: Selections,
                                      context: LogicalPlanningContext,
                                      // This function is used when the aggregation needs a specific label to exist,
                                      // for constraint checking
                                      labelCheck: Option[LabelName] => (Option[LogicalPlan] => Option[LogicalPlan]) = _ => identity): Option[LogicalPlan] = {
    if (patternRelationships.isEmpty &&
      variableName.forall(patternNodes.contains) &&
      noWrongPredicates(patternNodes, selections)) { // MATCH (n), MATCH (n:A)
      val lpp = context.logicalPlanProducer
      val labels = patternNodes.toList.map(n => findLabel(n, selections))
      val aggregation1 = lpp.planCountStoreNodeAggregation(query, columnName, labels, argumentIds, context)
      labels.collectFirst {
        case l if labelCheck(l)(Some(aggregation1)).nonEmpty => aggregation1
      }
    } else if (patternRelationships.size == 1 && notLoop(patternRelationships.head)) { // MATCH ()-[r]->(), MATCH ()-[r:X]->(), MATCH ()-[r:X|Y]->()
      labelCheck(None)(
        trySolveRelationshipAggregation(query, columnName, variableName, patternRelationships, argumentIds, selections, context)
      )
    } else None
  }

  // the counts store counts loops twice
  private def notLoop(r: PatternRelationship) = r.nodes._1 != r.nodes._2

  private def trySolveRelationshipAggregation(query: PlannerQuery, columnName: String, variableName: Option[String],
                                              patternRelationships: Set[PatternRelationship], argumentIds: Set[String],
                                              selections: Selections, context: LogicalPlanningContext): Option[LogicalPlan] = {
    patternRelationships.head match {

      case PatternRelationship(relId, (startNodeId, endNodeId), direction, types, SimplePatternLength)
        if variableName.forall(_ == relId) && noWrongPredicates(Set(startNodeId, endNodeId), selections) =>

        def planRelAggr(fromLabel: Option[LabelName], toLabel: Option[LabelName]) =
          Some(context.logicalPlanProducer.planCountStoreRelationshipAggregation(query, columnName, fromLabel, types, toLabel, argumentIds, context))

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

  def noWrongPredicates(nodeIds: Set[String], selections: Selections): Boolean = {
    val (labelPredicates, other) = selections.predicates.partition {
      case Predicate(nIds, h: HasLabels) if nIds.forall(nodeIds.contains) && h.labels.size == 1 => true
      case _ => false
    }
    labelPredicates.size <= nodeIds.size && other.isEmpty
  }

  def findLabel(nodeId: String, selections: Selections): Option[LabelName] = selections.predicates.collectFirst {
    case Predicate(nIds, h: HasLabels) if nIds == Set(nodeId) && h.labels.size == 1 => h.labels.head
  }
}
