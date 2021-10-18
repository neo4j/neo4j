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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.RelationshipLeafPlanner.planHiddenSelectionAndRelationshipLeafPlan
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

case class idSeekLeafPlanner(skipIDs: Set[String]) extends LeafPlanner {

  override def apply(queryGraph: QueryGraph,
                     interestingOrderConfig: InterestingOrderConfig,
                     context: LogicalPlanningContext): Set[LogicalPlan] = {
    queryGraph.selections.flatPredicatesSet.flatMap { e =>
      val arguments: Set[LogicalVariable] = queryGraph.argumentIds.map(n => Variable(n)(null))
      val idSeekPredicates: Option[(Expression, LogicalVariable, SeekableArgs)] = e match {
        // MATCH (a)-[r]-(b) WHERE id(r) IN expr
        // MATCH a WHERE id(a) IN {param}
        case predicate@AsIdSeekable(seekable) if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
          Some((predicate, seekable.ident, seekable.args))
        case _ => None
      }

      idSeekPredicates flatMap {
        case (predicate, variable@Variable(id), idValues) if !queryGraph.argumentIds.contains(id) =>
          if (skipIDs.contains(id)) {
            None
          } else {
            queryGraph.patternRelationships.find(_.name == id) match {
              case Some(relationship) =>
                Some(planHiddenSelectionAndRelationshipLeafPlan(
                  queryGraph,
                  relationship,
                  context,
                  planRelationshipByIdSeek(variable, _, _, _, idValues, Seq(predicate), queryGraph.argumentIds, context)
                ))

              case None =>
                Some(context.logicalPlanProducer.planNodeByIdSeek(variable, idValues, Seq(predicate), queryGraph.argumentIds, context))
            }
          }
      }
    }
  }

  private def planRelationshipByIdSeek(idExpr: Variable,
                                       patternForLeafPlan: PatternRelationship,
                                       originalPattern: PatternRelationship,
                                       hiddenSelections: Seq[Expression],
                                       idValues: SeekableArgs,
                                       predicates: Seq[Expression],
                                       argumentIds: Set[String],
                                       context: LogicalPlanningContext): LogicalPlan = {
    val name = originalPattern.name
    val relTypeFilterHiddenSelection = relTypeFilter(idExpr, originalPattern.types.toList)
    context.logicalPlanProducer.planRelationshipByIdSeek(
      name,
      idValues,
      patternForLeafPlan,
      originalPattern,
      hiddenSelections ++ relTypeFilterHiddenSelection,
      argumentIds,
      predicates,
      context
    )
  }

  private def relTypeFilter(idExpr: Variable, relTypes: List[RelTypeName]): Option[Expression] = {
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

  private def relTypeAsStringLiteral(relType: RelTypeName) = StringLiteral(relType.name)(relType.position)

  private def typeOfRelExpr(idExpr: Variable) =
    FunctionInvocation(FunctionName("type")(idExpr.position), idExpr)(idExpr.position)
}

