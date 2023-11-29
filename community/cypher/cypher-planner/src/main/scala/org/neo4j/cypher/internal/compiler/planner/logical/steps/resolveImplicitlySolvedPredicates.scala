/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsExplicitlyPropertyScannable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PropertyTypeMapper

case object resolveImplicitlySolvedPredicates extends SelectionCandidateGenerator {

  override def apply(
    input: LogicalPlan,
    unsolvedPredicates: Set[Expression],
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Iterator[SelectionCandidate] = {
    val unsolvedNotNullPredicates = unsolvedPredicates.collect { case e @ AsExplicitlyPropertyScannable(scannable) =>
      (scannable.ident, scannable.property.propertyKey.name, e)
    }
    val unsolvedIsTypedPredicates =
      unsolvedPredicates.collect { case e @ IsTyped(property @ Property(variable: LogicalVariable, _), _) =>
        IsTypedPredicateCandidate(variable, property.propertyKey.name, e)
      }

    val implicitlySolvedPredicates = solvedNodePropertyNotNullPredicates(input, unsolvedNotNullPredicates, context) ++
      solvedRelationshipPropertyNotNullPredicates(input, unsolvedNotNullPredicates, context) ++
      solvedNodePropertyIsTypedPredicates(input, unsolvedIsTypedPredicates, context) ++
      solvedRelationshipPropertyIsTypedPredicates(input, unsolvedIsTypedPredicates, context)

    if (implicitlySolvedPredicates.isEmpty) {
      Iterator.empty
    } else {
      val plan = context.staticComponents.logicalPlanProducer.solvePredicates(input, implicitlySolvedPredicates)
      Iterator(SelectionCandidate(plan, implicitlySolvedPredicates))
    }
  }

  case class IsTypedPredicateCandidate(variable: LogicalVariable, property: String, predicate: IsTyped)

  def solvedNodePropertyNotNullPredicates(
    plan: LogicalPlan,
    unsolvedNotNullPredicates: Set[(LogicalVariable, String, Expression)],
    context: LogicalPlanningContext
  ): Set[Expression] = {
    lazy val solvedLabelPredicates =
      context.staticComponents.planningAttributes.solveds(plan.id).asSinglePlannerQuery.queryGraph.selections.labelInfo
    for {
      (variable, property, predicate) <- unsolvedNotNullPredicates
      labelName <- solvedLabelPredicates.getOrElse(variable, Set.empty)
      if context.staticComponents.planContext.hasNodePropertyExistenceConstraint(labelName.name, property)
    } yield predicate
  }

  def solvedNodePropertyIsTypedPredicates(
    plan: LogicalPlan,
    unsolvedIsTypedPredicates: Set[IsTypedPredicateCandidate],
    context: LogicalPlanningContext
  ): Set[IsTyped] = {
    lazy val solvedLabelPredicates =
      context.staticComponents.planningAttributes.solveds(plan.id).asSinglePlannerQuery.queryGraph.selections.labelInfo
    for {
      predicateCandidate <- unsolvedIsTypedPredicates
      if predicateCandidate.predicate.typeName.isNullable
      label <- solvedLabelPredicates.getOrElse(predicateCandidate.variable, Set.empty)
      propertyType <- PropertyTypeMapper.asSchemaValueType(predicateCandidate.predicate.typeName)
      if context.staticComponents.planContext.hasNodePropertyTypeConstraint(
        label.name,
        predicateCandidate.property,
        propertyType
      )
    } yield predicateCandidate.predicate
  }

  def solvedRelationshipPropertyNotNullPredicates(
    plan: LogicalPlan,
    unsolvedNotNullPredicates: Set[(LogicalVariable, String, Expression)],
    context: LogicalPlanningContext
  ): Set[Expression] = {
    lazy val qg = context.staticComponents.planningAttributes.solveds(plan.id).asSinglePlannerQuery.queryGraph
    lazy val solvedRelTypePredicates =
      qg.selections.relTypeInfo ++ qg.patternRelationshipTypes.map(x => (x._1, Set(x._2)))

    for {
      (variable, property, predicate) <- unsolvedNotNullPredicates
      typeName <- solvedRelTypePredicates.getOrElse(variable, Set.empty)
      if context.staticComponents.planContext.hasRelationshipPropertyExistenceConstraint(typeName.name, property)
    } yield predicate
  }

  def solvedRelationshipPropertyIsTypedPredicates(
    plan: LogicalPlan,
    unsolvedIsTypedPredicates: Set[IsTypedPredicateCandidate],
    context: LogicalPlanningContext
  ): Set[IsTyped] = {
    lazy val qg = context.staticComponents.planningAttributes.solveds(plan.id).asSinglePlannerQuery.queryGraph
    lazy val solvedRelTypePredicates =
      qg.selections.relTypeInfo ++ qg.patternRelationshipTypes.map(x => (x._1, Set(x._2)))

    for {
      predicateCandidate <- unsolvedIsTypedPredicates
      if predicateCandidate.predicate.typeName.isNullable
      relType <- solvedRelTypePredicates.getOrElse(predicateCandidate.variable, Set.empty)
      propertyType <- PropertyTypeMapper.asSchemaValueType(predicateCandidate.predicate.typeName)
      if context.staticComponents.planContext.hasRelationshipPropertyTypeConstraint(
        relType.name,
        predicateCandidate.property,
        propertyType
      )
    } yield predicateCandidate.predicate
  }
}
