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

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsElementIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.RelationshipLeafPlanner.planHiddenSelectionAndRelationshipLeafPlan
import org.neo4j.cypher.internal.compiler.planner.logical.steps.idSeekLeafPlanner.IdType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.SeekableArgs

case class idSeekLeafPlanner(skipIDs: Set[LogicalVariable]) extends LeafPlanner {

  override def apply(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    queryGraph.selections.flatPredicatesSet.flatMap { e =>
      val arguments: Set[LogicalVariable] = queryGraph.argumentIds
      val idSeekPredicates: Option[(Expression, LogicalVariable, SeekableArgs, IdType)] = e match {
        // MATCH (a)-[r]-(b) WHERE id(r) IN expr
        // MATCH (a) WHERE id(a) IN $param
        case predicate @ AsIdSeekable(seekable)
          if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
          Some((predicate, seekable.ident, seekable.args, IdType.Id))

        // MATCH (a)-[r]-(b) WHERE elementId(r) IN expr
        // MATCH (a) WHERE elementId(a) IN $param
        case predicate @ AsElementIdSeekable(seekable)
          if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
          Some((predicate, seekable.ident, seekable.args, IdType.ElementId))

        case _ => None
      }

      idSeekPredicates flatMap {
        case (predicate, variable, idValues, idType) =>
          if (skipIDs.contains(variable)) {
            None
          } else {
            queryGraph.patternRelationships.find(_.variable == variable) match {
              case Some(relationship) =>
                Some(planHiddenSelectionAndRelationshipLeafPlan(
                  queryGraph.argumentIds,
                  relationship,
                  context,
                  planRelationshipByIdSeek(
                    variable.asInstanceOf[Variable],
                    _,
                    _,
                    _,
                    idValues,
                    Seq(predicate),
                    queryGraph.argumentIds,
                    context,
                    idType
                  )
                ))

              case None =>
                val producePlan = idType match {
                  case IdType.Id        => context.staticComponents.logicalPlanProducer.planNodeByIdSeek _
                  case IdType.ElementId => context.staticComponents.logicalPlanProducer.planNodeByElementIdSeek _
                }

                Some(producePlan(
                  variable,
                  idValues,
                  Seq(predicate),
                  queryGraph.argumentIds,
                  context
                ))
            }
          }
      }
    }
  }

  private def planRelationshipByIdSeek(
    idExpr: LogicalVariable,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    hiddenSelections: Seq[Expression],
    idValues: SeekableArgs,
    predicates: Seq[Expression],
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext,
    idType: IdType
  ): LogicalPlan = {
    val producePlan = idType match {
      case IdType.Id        => context.staticComponents.logicalPlanProducer.planRelationshipByIdSeek _
      case IdType.ElementId => context.staticComponents.logicalPlanProducer.planRelationshipByElementIdSeek _
    }

    val variable = originalPattern.variable
    val relTypeFilterHiddenSelection = relTypeFilter(idExpr, originalPattern.types.toList)
    producePlan(
      variable,
      idValues,
      patternForLeafPlan,
      originalPattern,
      hiddenSelections ++ relTypeFilterHiddenSelection,
      argumentIds,
      predicates,
      context
    )
  }

  private def relTypeFilter(idExpr: LogicalVariable, relTypes: List[RelTypeName]): Option[Expression] = {
    relTypes match {
      case Seq(tpe) =>
        val relTypeExpr = relTypeAsStringLiteral(tpe)
        Some(In(typeOfRelExpr(idExpr), relTypeExpr)(idExpr.position))

      case _ :: _ =>
        val relTypeExprs = relTypes.map(relTypeAsStringLiteral)
        val invocation = typeOfRelExpr(idExpr)
        val idPos = idExpr.position
        Some(In(invocation, ListLiteral(relTypeExprs)(relTypeExprs.head.position))(idPos))

      case _ =>
        None
    }
  }

  private def relTypeAsStringLiteral(relType: RelTypeName) =
    StringLiteral(relType.name)(relType.position.withInputLength(0))

  private def typeOfRelExpr(idExpr: LogicalVariable) =
    FunctionInvocation(FunctionName("type")(idExpr.position), idExpr)(idExpr.position)
}

object idSeekLeafPlanner {
  sealed trait IdType

  object IdType {
    case object Id extends IdType
    case object ElementId extends IdType
  }
}
